package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"AuditReady-k3s/internal/metrics"
)

// AuthError is returned for 401 (bad/revoked token) and 403 (cluster
// mismatch) responses. These are fatal for the request and retrying without
// new credentials is pointless — callers should back off hard and alert.
type AuthError struct {
	Status int
	Body   string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("control plane authentication failed (HTTP %d): %s", e.Status, e.Body)
}

// Client talks to the AuditReady control plane over plain HTTP POST with a
// long-lived bearer token. No bootstrap, no certificates — TLS is whatever
// the endpoint URL provides.
type Client struct {
	base  string // e.g. https://server/audit_ready
	token string
	hc    *http.Client
	log   *slog.Logger
}

// NewClient builds a Client for endpoint (the /audit_ready base URL;
// trailing slashes are trimmed) with the given bearer token.
func NewClient(endpoint, token string, log *slog.Logger) *Client {
	return &Client{
		base:  strings.TrimRight(endpoint, "/"),
		token: token,
		hc:    &http.Client{Timeout: 30 * time.Second},
		log:   log,
	}
}

// Inventory POSTs a batch to /k8s/inventory and returns the server's ack.
func (c *Client) Inventory(ctx context.Context, batch *InventoryBatch) (*InventoryAck, error) {
	ack := new(InventoryAck)
	if err := c.post(ctx, "/k8s/inventory", batch, ack); err != nil {
		return nil, err
	}
	return ack, nil
}

// Poll POSTs reports to /k8s/poll and returns queued commands.
func (c *Client) Poll(ctx context.Context, req *PollRequest) (*PollResponse, error) {
	resp := new(PollResponse)
	if err := c.post(ctx, "/k8s/poll", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Client) post(ctx context.Context, path string, reqBody, respBody any) error {
	raw, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build %s request: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.hc.Do(req)
	if err != nil {
		metrics.SetTunnelConnected(false)
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		metrics.SetTunnelConnected(false)
		return fmt.Errorf("read %s response: %w", path, err)
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		metrics.SetTunnelConnected(true)
		if err := json.Unmarshal(body, respBody); err != nil {
			return fmt.Errorf("decode %s response: %w", path, err)
		}
		return nil
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		metrics.SetTunnelConnected(false)
		return &AuthError{Status: resp.StatusCode, Body: truncate(string(body), 200)}
	default:
		metrics.SetTunnelConnected(false)
		return fmt.Errorf("POST %s: HTTP %d: %s", path, resp.StatusCode, truncate(string(body), 200))
	}
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
