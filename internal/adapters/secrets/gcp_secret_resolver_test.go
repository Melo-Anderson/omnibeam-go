package secrets_test

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestGCPSecretManagerResolver_NormalizeResource(t *testing.T) {
	cfg := &domain.SecretsConfig{
		GCPProjectID: "test-project-123",
	}
	resolver := &secrets.GCPSecretManagerResolver{}
	resolver.SetProjectID(cfg.GCPProjectID)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Short secret name",
			input:    "my-db-secret",
			expected: "projects/test-project-123/secrets/my-db-secret/versions/latest",
		},
		{
			name:     "Full resource name",
			input:    "projects/custom-proj/secrets/db-pass/versions/2",
			expected: "projects/custom-proj/secrets/db-pass/versions/2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolver.NormalizeResourcePath(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.expected {
				t.Errorf("NormalizeResourcePath(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestGCPSecretManagerResolver_ExtractField(t *testing.T) {
	rawJSON := `{"password":"cloud-secret-pass","user":"cloud-user"}`
	val, err := secrets.ExtractGCPPayloadField([]byte(rawJSON), "password")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "cloud-secret-pass" {
		t.Errorf("expected cloud-secret-pass, got %q", val)
	}

	// Invalid JSON error
	_, err = secrets.ExtractGCPPayloadField([]byte("not-json"), "password")
	if err == nil {
		t.Error("expected error for non-json payload in ExtractGCPPayloadField")
	}

	// Missing field error
	_, err = secrets.ExtractGCPPayloadField([]byte(rawJSON), "missing_key")
	if err == nil {
		t.Error("expected error for missing field in ExtractGCPPayloadField")
	}
}

func TestGCPSecretManagerResolver_UnitLifecycleAndResolution(t *testing.T) {
	ctx := context.Background()

	t.Run("NormalizeResourcePath errors when projectID is empty", func(t *testing.T) {
		resolver := &secrets.GCPSecretManagerResolver{}
		_, err := resolver.NormalizeResourcePath("short-secret")
		if err == nil {
			t.Error("expected error for empty projectID in NormalizeResourcePath")
		}
	})

	t.Run("Resolve errors when client is uninitialized", func(t *testing.T) {
		resolver := &secrets.GCPSecretManagerResolver{}
		resolver.SetProjectID("test-project")
		_, err := resolver.Resolve(ctx, "projects/test-project/secrets/my-sec/versions/1")
		if err == nil {
			t.Error("expected error when client is nil in Resolve")
		}
	})

	t.Run("Close returns nil when client is nil", func(t *testing.T) {
		resolver := &secrets.GCPSecretManagerResolver{}
		if err := resolver.Close(); err != nil {
			t.Errorf("expected nil error on Close with nil client, got %v", err)
		}
	})
}
