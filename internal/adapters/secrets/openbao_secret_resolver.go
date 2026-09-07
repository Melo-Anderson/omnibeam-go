// Package secrets implements enterprise secret management and credential resolution.
package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Compile-time assertion of interface satisfaction (LSP).
var _ ports.SecretResolver = (*OpenBaoSecretResolver)(nil)

// OpenBaoSecretResolver resolves credentials from HashiCorp Vault / OpenBao endpoints.
type OpenBaoSecretResolver struct {
	client       *http.Client
	vaultURL     string
	vaultToken   string
	hostOverride map[string]string
	tracer       trace.Tracer
}

// NewOpenBaoSecretResolver creates an instance with fallback hierarchy resolution.
func NewOpenBaoSecretResolver(cfg *domain.SecretsConfig) *OpenBaoSecretResolver {
	vaultURL := resolveVaultURL(cfg)
	vaultToken := resolveVaultToken(cfg)

	var hostOverride map[string]string
	if cfg != nil {
		hostOverride = cfg.HostOverride
	}

	return &OpenBaoSecretResolver{
		client:       &http.Client{Timeout: 10 * time.Second},
		vaultURL:     strings.TrimRight(vaultURL, "/"),
		vaultToken:   vaultToken,
		hostOverride: hostOverride,
		tracer:       otel.Tracer("openbao-secret-resolver"),
	}
}

func resolveVaultURL(cfg *domain.SecretsConfig) string {
	if cfg != nil && strings.TrimSpace(cfg.VaultURL) != "" {
		return cfg.VaultURL
	}
	for _, env := range []string{"PLATFORM_VAULT_URL", "BAO_ADDR", "VAULT_ADDR"} {
		if val := strings.TrimSpace(os.Getenv(env)); val != "" {
			return val
		}
	}
	return ""
}

func resolveVaultToken(cfg *domain.SecretsConfig) string {
	if cfg != nil && strings.TrimSpace(cfg.VaultToken) != "" {
		return cfg.VaultToken
	}
	for _, env := range []string{"PLATFORM_VAULT_TOKEN", "BAO_TOKEN", "VAULT_TOKEN"} {
		if val := strings.TrimSpace(os.Getenv(env)); val != "" {
			return val
		}
	}
	return ""
}

// Resolve resolves a secret reference from OpenBao.
func (r *OpenBaoSecretResolver) Resolve(ctx context.Context, secretRef string) (string, error) {
	ctx, span := r.tracer.Start(ctx, "OpenBaoSecretResolver.Resolve")
	defer span.End()

	if r.vaultURL == "" {
		err := fmt.Errorf("openbao/vault endpoint URL is not configured (specify vault_url in secrets_config or set PLATFORM_VAULT_URL / BAO_ADDR / VAULT_ADDR)")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	parsed := ParseSecretRef(secretRef)
	span.SetAttributes(
		attribute.String("secrets.provider", "openbao"),
		attribute.String("secrets.path", parsed.Path),
		attribute.Bool("secrets.has_field", parsed.Field != ""),
	)

	normPath, wasNormalized := NormalizeVaultPath(parsed.Path)
	payload, err := r.fetchSecret(ctx, normPath)
	if err != nil && wasNormalized {
		// Fallback to direct path for KV v1 compatibility
		payload, err = r.fetchSecret(ctx, parsed.Path)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("failed resolving openbao secret %q: %w", parsed.Path, err)
	}

	val, err := r.extractValue(payload, parsed.Field)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	return val, nil
}

func (r *OpenBaoSecretResolver) fetchSecret(ctx context.Context, apiPath string) (map[string]any, error) {
	if r.vaultURL == "" {
		return nil, fmt.Errorf("openbao/vault endpoint URL is not configured")
	}
	reqURL := fmt.Sprintf("%s/v1/%s", r.vaultURL, strings.TrimPrefix(apiPath, "/"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed creating http request: %w", err)
	}

	if r.vaultToken != "" {
		req.Header.Set("X-Vault-Token", r.vaultToken)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vault returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}

	return unpackEnvelope(body)
}

func unpackEnvelope(body []byte) (map[string]any, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed parsing vault json: %w", err)
	}

	if dataObj, ok := raw["data"].(map[string]any); ok {
		if innerData, ok := dataObj["data"].(map[string]any); ok {
			return innerData, nil // KV v2
		}
		return dataObj, nil // KV v1
	}
	return raw, nil
}

func (r *OpenBaoSecretResolver) extractValue(payload map[string]any, field string) (string, error) {
	if field != "" {
		val, ok := payload[field]
		if !ok {
			return "", fmt.Errorf("field %q not found in secret payload", field)
		}
		return fmt.Sprintf("%v", val), nil
	}

	ApplyHostOverride(payload, r.hostOverride)
	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed marshaling secret map: %w", err)
	}
	return string(bytes), nil
}
