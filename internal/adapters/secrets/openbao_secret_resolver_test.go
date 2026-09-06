package secrets_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestOpenBaoSecretResolver_Resolve(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "test-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		switch r.URL.Path {
		case "/v1/secret/data/postgres":
			// KV v2 envelope
			resp := map[string]any{
				"data": map[string]any{
					"data": map[string]any{
						"password": "secret-postgres-pass",
						"user":     "postgres-user",
						"host":     "postgres",
					},
				},
			}
			json.NewEncoder(w).Encode(resp)

		case "/v1/secret/data/kv1-only":
			// 404 to trigger fallback
			w.WriteHeader(http.StatusNotFound)

		case "/v1/secret/kv1-only":
			// KV v1 envelope
			resp := map[string]any{
				"data": map[string]any{
					"password": "kv1-secret-pass",
				},
			}
			json.NewEncoder(w).Encode(resp)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	cfg := &domain.SecretsConfig{
		VaultURL:   server.URL,
		VaultToken: "test-token",
		HostOverride: map[string]string{
			"postgres": "localhost",
		},
	}
	resolver := secrets.NewOpenBaoSecretResolver(cfg)

	t.Run("Resolves specific field with auto-path normalization (KV v2)", func(t *testing.T) {
		val, err := resolver.Resolve(context.Background(), "secret/postgres#password")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "secret-postgres-pass" {
			t.Errorf("expected secret-postgres-pass, got %q", val)
		}
	})

	t.Run("Resolves full map JSON with host override", func(t *testing.T) {
		val, err := resolver.Resolve(context.Background(), "secret/postgres")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(val), &payload); err != nil {
			t.Fatalf("failed unmarshaling resolved JSON map: %v", err)
		}
		if payload["host"] != "localhost" {
			t.Errorf("expected host override localhost, got %v", payload["host"])
		}
	})

	t.Run("Falls back to KV v1 on 404 from /data/ endpoint", func(t *testing.T) {
		val, err := resolver.Resolve(context.Background(), "secret/kv1-only#password")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "kv1-secret-pass" {
			t.Errorf("expected kv1-secret-pass, got %q", val)
		}
	})

	t.Run("Error when field not found in secret map", func(t *testing.T) {
		_, err := resolver.Resolve(context.Background(), "secret/postgres#nonexistent_field")
		if err == nil {
			t.Error("expected error for missing field, got nil")
		}
	})

	t.Run("Error when secret does not exist (404)", func(t *testing.T) {
		_, err := resolver.Resolve(context.Background(), "secret/totally_missing")
		if err == nil {
			t.Error("expected error for missing secret, got nil")
		}
	})

	t.Run("Resolves Vault URL and Token from environment variables", func(t *testing.T) {
		t.Setenv("VAULT_ADDR", server.URL)
		t.Setenv("VAULT_TOKEN", "test-token")
		envResolver := secrets.NewOpenBaoSecretResolver(nil)
		val, err := envResolver.Resolve(context.Background(), "secret/postgres#password")
		if err != nil || val != "secret-postgres-pass" {
			t.Errorf("unexpected resolution with env vars: val=%s, err=%v", val, err)
		}
	})

	t.Run("Missing Vault URL returns error", func(t *testing.T) {
		t.Setenv("PLATFORM_VAULT_URL", "")
		t.Setenv("BAO_ADDR", "")
		t.Setenv("VAULT_ADDR", "")
		t.Setenv("VAULT_TOKEN", "token")
		noURLResolver := secrets.NewOpenBaoSecretResolver(&domain.SecretsConfig{})
		_, err := noURLResolver.Resolve(context.Background(), "secret/test")
		if err == nil {
			t.Error("expected error when vault URL is missing")
		}
	})

	t.Run("Missing Vault Token returns error", func(t *testing.T) {
		t.Setenv("VAULT_TOKEN", "")
		t.Setenv("BAO_TOKEN", "")
		t.Setenv("PLATFORM_VAULT_TOKEN", "")
		noTokenResolver := secrets.NewOpenBaoSecretResolver(&domain.SecretsConfig{VaultURL: "http://localhost:8200"})
		_, err := noTokenResolver.Resolve(context.Background(), "secret/test")
		if err == nil {
			t.Error("expected error when vault token is missing")
		}
	})

	t.Run("Server 500 error propagation", func(t *testing.T) {
		errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		}))
		defer errServer.Close()

		errResolver := secrets.NewOpenBaoSecretResolver(&domain.SecretsConfig{
			VaultURL:   errServer.URL,
			VaultToken: "valid-token",
		})
		_, err := errResolver.Resolve(context.Background(), "secret/failing")
		if err == nil {
			t.Error("expected error when vault server returns 500")
		}
	})
}
