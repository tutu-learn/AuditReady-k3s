package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestPublicKeyAcceptsBase64AndHex(t *testing.T) {
	_, raw, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pub := raw.Public().(ed25519.PublicKey)

	for name, encoded := range map[string]string{
		"base64": base64.StdEncoding.EncodeToString(pub),
		"hex":    hex.EncodeToString(pub),
	} {
		t.Run(name, func(t *testing.T) {
			c := &Config{ServerPublicKey: encoded}
			got, err := c.PublicKey()
			if err != nil {
				t.Fatalf("PublicKey() error: %v", err)
			}
			if !got.Equal(pub) {
				t.Fatalf("PublicKey() mismatch")
			}
		})
	}

	c := &Config{ServerPublicKey: "not-a-key"}
	if _, err := c.PublicKey(); err == nil {
		t.Fatal("expected error for garbage key")
	}
}
