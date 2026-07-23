package protocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// SigningPayload returns the canonical byte string that the control plane
// signs and the agent verifies. It covers every field that affects what the
// command does; the signature itself is excluded.
//
// Format (newline-separated, no trailing newline):
//
//	nonce, timestamp, id, verb,
//	target.group, target.version, target.resource, target.namespace, target.name,
//	patch_type, sha256hex(payload), expected_hash, override,
//	[helm.release_name, helm.namespace, helm.chart_ref, helm.version, sha256hex(helm.values_yaml)]
func (c *Command) SigningPayload() []byte {
	payloadHash := sha256.Sum256(c.Payload)
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n%d\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%x\n%s\n%t",
		c.Nonce,
		c.Timestamp,
		c.ID,
		c.Verb,
		c.Target.Group,
		c.Target.Version,
		c.Target.Resource,
		c.Target.Namespace,
		c.Target.Name,
		c.PatchType,
		payloadHash,
		c.ExpectedHash,
		c.Override,
	)
	if c.Helm != nil {
		valuesHash := sha256.Sum256(c.Helm.ValuesYAML)
		fmt.Fprintf(&b, "\n%s\n%s\n%s\n%s\n%x",
			c.Helm.ReleaseName,
			c.Helm.Namespace,
			c.Helm.ChartRef,
			c.Helm.Version,
			valuesHash,
		)
	}
	return []byte(b.String())
}

// Sign sets c.Signature using the given Ed25519 private key.
func (c *Command) Sign(priv ed25519.PrivateKey) {
	sig := ed25519.Sign(priv, c.SigningPayload())
	c.Signature = base64.StdEncoding.EncodeToString(sig)
}

// VerifySignature checks c.Signature against the given Ed25519 public key.
func (c *Command) VerifySignature(pub ed25519.PublicKey) error {
	if c.Signature == "" {
		return errors.New("command has no signature")
	}
	sig, err := base64.StdEncoding.DecodeString(c.Signature)
	if err != nil {
		return fmt.Errorf("signature is not valid base64: %w", err)
	}
	if !ed25519.Verify(pub, c.SigningPayload(), sig) {
		return errors.New("invalid command signature")
	}
	return nil
}
