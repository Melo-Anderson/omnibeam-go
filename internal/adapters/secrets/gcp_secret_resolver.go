// Package secrets implements enterprise secret management and credential resolution.
package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Compile-time assertion of interface satisfaction (LSP).
var _ ports.SecretResolver = (*GCPSecretManagerResolver)(nil)

// GCPSecretManagerResolver resolves credentials from Google Cloud Secret Manager.
type GCPSecretManagerResolver struct {
	projectID string
	client    *secretmanager.Client
	tracer    trace.Tracer
}

// NewGCPSecretManagerResolver creates a resolver and dials GCP Secret Manager.
func NewGCPSecretManagerResolver(ctx context.Context, cfg *domain.SecretsConfig) (*GCPSecretManagerResolver, error) {
	projectID := resolveGCPProject(cfg)
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed creating gcp secret manager client: %w", err)
	}
	return &GCPSecretManagerResolver{
		projectID: projectID,
		client:    client,
		tracer:    otel.Tracer("gcp-secret-manager-resolver"),
	}, nil
}

// SetProjectID updates the GCP project ID (useful for testing/configuration).
func (r *GCPSecretManagerResolver) SetProjectID(projectID string) {
	r.projectID = projectID
}

func resolveGCPProject(cfg *domain.SecretsConfig) string {
	if cfg != nil && strings.TrimSpace(cfg.GCPProjectID) != "" {
		return cfg.GCPProjectID
	}
	for _, env := range []string{"GCP_PROJECT_ID", "GOOGLE_CLOUD_PROJECT"} {
		if val := strings.TrimSpace(os.Getenv(env)); val != "" {
			return val
		}
	}
	return ""
}

// NormalizeResourcePath converts short secret names to full GCP Secret Manager resource paths.
func (r *GCPSecretManagerResolver) NormalizeResourcePath(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if strings.HasPrefix(trimmed, "projects/") {
		return trimmed, nil
	}
	if r.projectID == "" {
		return "", fmt.Errorf("gcp project id is not configured (specify gcp_project_id in secrets_config or set GCP_PROJECT_ID / GOOGLE_CLOUD_PROJECT)")
	}
	return fmt.Sprintf("projects/%s/secrets/%s/versions/latest", r.projectID, trimmed), nil
}

// Close releases the underlying GCP client connection.
func (r *GCPSecretManagerResolver) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

// Resolve retrieves secret payload from GCP Secret Manager.
func (r *GCPSecretManagerResolver) Resolve(ctx context.Context, secretRef string) (string, error) {
	tracer := r.tracer
	if tracer == nil {
		tracer = otel.Tracer("gcp-secret-manager-resolver")
	}
	ctx, span := tracer.Start(ctx, "GCPSecretManagerResolver.Resolve")
	defer span.End()

	parsed := ParseSecretRef(secretRef)
	resourceName, err := r.NormalizeResourcePath(parsed.Path)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	span.SetAttributes(
		attribute.String("secrets.provider", "gcp_secret_manager"),
		attribute.String("secrets.resource", resourceName),
	)

	if r.client == nil {
		err := fmt.Errorf("gcp secret manager client is not initialized")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: resourceName,
	}

	result, err := r.client.AccessSecretVersion(ctx, req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("failed accessing gcp secret %q: %w", resourceName, err)
	}

	if parsed.Field != "" {
		val, err := ExtractGCPPayloadField(result.Payload.Data, parsed.Field)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return "", err
		}
		return val, nil
	}

	return string(result.Payload.Data), nil
}

// ExtractGCPPayloadField parses a JSON payload and returns the value of the requested field.
func ExtractGCPPayloadField(data []byte, field string) (string, error) {
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("secret payload is not valid json for field extraction: %w", err)
	}
	val, ok := payload[field]
	if !ok {
		return "", fmt.Errorf("field %q not found in gcp secret payload", field)
	}
	return fmt.Sprintf("%v", val), nil
}
