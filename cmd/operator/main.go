// Command operator is the in-cluster k8s-agent: it uploads cluster state to
// the AuditReady control plane over outbound HTTPS and executes signed
// commands it polls from it. The read path uses the agent-reader
// ServiceAccount token, the write path the agent-writer token; without a
// writer token the process runs in read-only mode and refuses all commands.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"AuditReady-k3s/internal/config"
	"AuditReady-k3s/internal/executor"
	"AuditReady-k3s/internal/informers"
	"AuditReady-k3s/internal/protocol"
	"AuditReady-k3s/internal/receiver"
	"AuditReady-k3s/internal/server"
	"AuditReady-k3s/internal/uploader"
	"AuditReady-k3s/internal/wsclient"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "operator: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	log.Info("starting k8s-agent operator", "endpoint", cfg.EndpointBase(), "writerEnabled", cfg.WriterEnabled, "readOnly", !cfg.WriterEnabled)

	// kubeConfigFor builds a rest.Config authenticated with the given SA token
	// file. With KUBECONFIG set (local development) both paths share the
	// kubeconfig credentials instead.
	kubeConfigFor := func(tokenPath string) (*rest.Config, error) {
		if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
			return clientcmd.BuildConfigFromFlags("", kubeconfig)
		}
		rc, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
		rc.BearerToken = ""
		rc.BearerTokenFile = tokenPath
		return rc, nil
	}

	readerCfg, err := kubeConfigFor(cfg.ReaderTokenPath)
	if err != nil {
		return fmt.Errorf("reader kube config: %w", err)
	}
	readerDyn, err := dynamic.NewForConfig(readerCfg)
	if err != nil {
		return fmt.Errorf("reader dynamic client: %w", err)
	}
	disco, err := discovery.NewDiscoveryClientForConfig(readerCfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}

	// The write path requires the writer token; without it the operator runs
	// read-only regardless of configuration.
	writerAvailable := cfg.WriterEnabled
	if _, err := os.Stat(cfg.WriterTokenPath); err != nil && os.Getenv("KUBECONFIG") == "" {
		if cfg.WriterEnabled {
			log.Warn("writer token not found, forcing read-only mode", "path", cfg.WriterTokenPath)
		}
		writerAvailable = false
	}
	cfg.WriterEnabled = writerAvailable

	var exec *executor.Executor
	if writerAvailable {
		writerCfg, err := kubeConfigFor(cfg.WriterTokenPath)
		if err != nil {
			return fmt.Errorf("writer kube config: %w", err)
		}
		writerDyn, err := dynamic.NewForConfig(writerCfg)
		if err != nil {
			return fmt.Errorf("writer dynamic client: %w", err)
		}
		writerKube, err := kubernetes.NewForConfig(writerCfg)
		if err != nil {
			return fmt.Errorf("writer clientset: %w", err)
		}
		exec = executor.New(cfg, writerDyn, writerKube, writerCfg, log.With("subsystem", "executor"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := protocol.NewClient(cfg.EndpointBase(), cfg.ServerToken, log.With("subsystem", "protocol"))
	// The uploader uploads what the informer manager collects, and resyncs
	// from the manager's cache after every outage — a lazy snapshotter breaks
	// the construction cycle.
	snap := &lazySnapshotter{}
	upload := uploader.New(cfg, client.Inventory, snap, log.With("subsystem", "uploader"))

	resources, err := informers.Resolve(cfg.InformersEnabled)
	if err != nil {
		return fmt.Errorf("resolve informers: %w", err)
	}
	manager := informers.New(readerDyn, disco, resources, upload, log.With("subsystem", "informers"))
	snap.m = manager

	// The WebSocket push channel is the fast path for commands and reports;
	// HTTP polling stays on as the fallback. nil push = poll-only mode.
	var push receiver.PushChannel
	var ws *wsclient.Client
	if cfg.WSEnabled {
		ws = wsclient.New(cfg, log.With("subsystem", "wsclient"))
		push = ws
	}
	recv := receiver.New(cfg, client.Poll, exec, log.With("subsystem", "receiver"), push)

	health := server.New()

	var wg sync.WaitGroup
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				log.Error("subsystem exited with error", "subsystem", name, "error", err)
				stop()
			}
		}()
	}
	if ws != nil {
		run("wsclient", func(ctx context.Context) error { ws.Run(ctx); return nil })
	}
	run("informers", func(ctx context.Context) error { manager.Run(ctx); return nil })
	run("uploader", func(ctx context.Context) error { upload.Run(ctx); return nil })
	run("receiver", func(ctx context.Context) error { recv.Run(ctx); return nil })
	run("http", func(ctx context.Context) error { return health.Run(ctx, cfg.MetricsAddr) })

	// Readiness: flip once every informer cache has synced.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if manager.HasSynced() {
					health.SetReady(true)
					log.Info("caches synced, operator ready")
					return
				}
			}
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	return nil
}

// lazySnapshotter lets the uploader resync from the informer manager's cache
// even though the manager is constructed after the uploader.
type lazySnapshotter struct {
	m *informers.Manager
}

func (l *lazySnapshotter) Snapshot() []*protocol.InventoryEvent {
	if l.m == nil {
		return nil
	}
	return l.m.Snapshot()
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
