package secrets_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestParseSecretRef(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected secrets.ParsedSecretRef
	}{
		{
			name:  "Prefixless path without field",
			input: "secret/postgres",
			expected: secrets.ParsedSecretRef{
				Provider: "",
				Path:     "secret/postgres",
				Field:    "",
			},
		},
		{
			name:  "Prefixless path with field",
			input: "secret/postgres#password",
			expected: secrets.ParsedSecretRef{
				Provider: "",
				Path:     "secret/postgres",
				Field:    "password",
			},
		},
		{
			name:  "Vault prefix with data subpath and field",
			input: "vault:secret/data/keys/pgp_financeiro#private_key",
			expected: secrets.ParsedSecretRef{
				Provider: "vault",
				Path:     "secret/data/keys/pgp_financeiro",
				Field:    "private_key",
			},
		},
		{
			name:  "OpenBao prefix",
			input: "openbao:secret/postgres#password",
			expected: secrets.ParsedSecretRef{
				Provider: "openbao",
				Path:     "secret/postgres",
				Field:    "password",
			},
		},
		{
			name:  "GCP Secret Manager full resource name",
			input: "gcp:projects/123/secrets/my-db-pass#password",
			expected: secrets.ParsedSecretRef{
				Provider: "gcp",
				Path:     "projects/123/secrets/my-db-pass",
				Field:    "password",
			},
		},
		{
			name:  "Plain literal without slash or provider",
			input: "plainsecretpassword",
			expected: secrets.ParsedSecretRef{
				Provider: "",
				Path:     "plainsecretpassword",
				Field:    "",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := secrets.ParseSecretRef(tc.input)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("ParseSecretRef(%q) = %+v, want %+v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestNormalizeVaultPath(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		wantPath       string
		wantNormalized bool
	}{
		{
			name:           "Injects /data/ when omitted",
			input:          "secret/postgres",
			wantPath:       "secret/data/postgres",
			wantNormalized: true,
		},
		{
			name:           "Preserves existing /data/",
			input:          "secret/data/postgres",
			wantPath:       "secret/data/postgres",
			wantNormalized: false,
		},
		{
			name:           "Nested path without /data/",
			input:          "secret/databases/primary/postgres",
			wantPath:       "secret/data/databases/primary/postgres",
			wantNormalized: true,
		},
		{
			name:           "Single segment path",
			input:          "postgres",
			wantPath:       "postgres",
			wantNormalized: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPath, gotNorm := secrets.NormalizeVaultPath(tc.input)
			if gotPath != tc.wantPath || gotNorm != tc.wantNormalized {
				t.Errorf("NormalizeVaultPath(%q) = (%q, %v), want (%q, %v)",
					tc.input, gotPath, gotNorm, tc.wantPath, tc.wantNormalized)
			}
		})
	}
}

func TestApplyHostOverride(t *testing.T) {
	payload := map[string]any{
		"host": "postgres",
		"port": 5432,
		"user": "testuser",
	}
	overrides := map[string]string{
		"postgres": "localhost",
	}

	secrets.ApplyHostOverride(payload, overrides)

	if payload["host"] != "localhost" {
		t.Errorf("expected host localhost, got %v", payload["host"])
	}
}

type fakeResolver struct {
	secrets map[string]string
}

func (f *fakeResolver) Resolve(_ context.Context, ref string) (string, error) {
	if val, ok := f.secrets[ref]; ok {
		return val, nil
	}
	return "", fmt.Errorf("secret %q not found", ref)
}

func TestResolveConnectionURI(t *testing.T) {
	ctx := context.Background()
	resolver := &fakeResolver{
		secrets: map[string]string{
			"db-pass-ref":           "supersecret",
			"openbao:secret/db#uri": "postgres://user:pass@remote:5432/db",
		},
	}

	t.Run("Interpolates password placeholder", func(t *testing.T) {
		uri, err := secrets.ResolveConnectionURI(ctx, "postgres://user:{{password}}@localhost:5432/db", "db-pass-ref", resolver)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := "postgres://user:supersecret@localhost:5432/db"
		if uri != expected {
			t.Errorf("expected %q, got %q", expected, uri)
		}
	})

	t.Run("Resolves full URI prefix", func(t *testing.T) {
		uri, err := secrets.ResolveConnectionURI(ctx, "openbao:secret/db#uri", "", resolver)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := "postgres://user:pass@remote:5432/db"
		if uri != expected {
			t.Errorf("expected %q, got %q", expected, uri)
		}
	})

	t.Run("Empty URI returns error", func(t *testing.T) {
		_, err := secrets.ResolveConnectionURI(ctx, "", "", resolver)
		if err == nil {
			t.Error("expected error for empty URI")
		}
	})
}

func TestApplyDatabaseHostOverrides(t *testing.T) {
	cfg := &domain.DatabaseSourceConfig{
		ConnectionURI: "postgres://user:pass@postgres:5432/db",
		Host:          "postgres",
	}
	overrides := map[string]string{"postgres": "localhost"}

	secrets.ApplyDatabaseHostOverrides(cfg, overrides)

	if cfg.ConnectionURI != "postgres://user:pass@localhost:5432/db" {
		t.Errorf("expected updated URI with localhost, got %q", cfg.ConnectionURI)
	}
	if cfg.Host != "localhost" {
		t.Errorf("expected host localhost, got %q", cfg.Host)
	}
}

func TestResolveAPIAuth(t *testing.T) {
	ctx := context.Background()
	resolver := &fakeResolver{
		secrets: map[string]string{
			"tok-1": "token123",
			"usr-1": "admin",
		},
	}

	auth := &domain.APIAuthConfig{
		TokenRef:    "tok-1",
		UsernameRef: "usr-1",
	}

	m := secrets.ResolveAPIAuth(ctx, auth, resolver)
	if m["tok-1"] != "token123" {
		t.Errorf("expected token123, got %q", m["tok-1"])
	}
	if m["usr-1"] != "admin" {
		t.Errorf("expected admin, got %q", m["usr-1"])
	}
}
