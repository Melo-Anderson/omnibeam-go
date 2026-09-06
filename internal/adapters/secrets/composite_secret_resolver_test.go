package secrets_test

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
)

type mockResolver struct {
	resolvedValue string
	err           error
}

func (m *mockResolver) Resolve(_ context.Context, _ string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.resolvedValue, nil
}

func TestCompositeSecretResolver(t *testing.T) {
	openbaoMock := &mockResolver{resolvedValue: "bao-pass"}
	gcpMock := &mockResolver{resolvedValue: "gcp-pass"}

	router := secrets.NewCompositeSecretResolver(openbaoMock, gcpMock, "openbao")

	t.Run("Routes prefixless path to default openbao resolver", func(t *testing.T) {
		val, err := router.Resolve(context.Background(), "secret/postgres#password")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "bao-pass" {
			t.Errorf("expected bao-pass, got %q", val)
		}
	})

	t.Run("Routes vault: prefix to openbao resolver", func(t *testing.T) {
		val, err := router.Resolve(context.Background(), "vault:secret/data/keys#key")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "bao-pass" {
			t.Errorf("expected bao-pass, got %q", val)
		}
	})

	t.Run("Routes gcp: prefix to gcp resolver", func(t *testing.T) {
		val, err := router.Resolve(context.Background(), "gcp:my-secret#password")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "gcp-pass" {
			t.Errorf("expected gcp-pass, got %q", val)
		}
	})

	t.Run("Passthrough literal string without slashes or provider", func(t *testing.T) {
		val, err := router.Resolve(context.Background(), "rawpassword123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "rawpassword123" {
			t.Errorf("expected rawpassword123, got %q", val)
		}
	})

	t.Run("Fails when prefixless path has no default provider configured", func(t *testing.T) {
		routerNoDefault := secrets.NewCompositeSecretResolver(openbaoMock, gcpMock, "")
		_, err := routerNoDefault.Resolve(context.Background(), "secret/postgres#password")
		if err == nil {
			t.Fatal("expected error when no default provider configured for path, got nil")
		}
	})

	t.Run("Fails for unsupported provider", func(t *testing.T) {
		_, err := router.Resolve(context.Background(), "aws:secret/postgres#password")
		if err == nil {
			t.Fatal("expected error for unsupported provider aws, got nil")
		}
	})
}

