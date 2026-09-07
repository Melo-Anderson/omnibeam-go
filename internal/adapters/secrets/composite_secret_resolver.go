// Package secrets implements enterprise secret management and credential resolution.
package secrets

import (
	"context"
	"fmt"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Compile-time assertion of interface satisfaction (LSP).
var _ ports.SecretResolver = (*CompositeSecretResolver)(nil)

// CompositeSecretResolver routes secret resolution to the appropriate backend.
type CompositeSecretResolver struct {
	openbao         ports.SecretResolver
	gcp             ports.SecretResolver
	defaultProvider string
	tracer          trace.Tracer
}

// NewCompositeSecretResolver constructs a polymorphic secret router.
func NewCompositeSecretResolver(openbao, gcp ports.SecretResolver, defaultProvider string) *CompositeSecretResolver {
	return &CompositeSecretResolver{
		openbao:         openbao,
		gcp:             gcp,
		defaultProvider: strings.ToLower(strings.TrimSpace(defaultProvider)),
		tracer:          otel.Tracer("composite-secret-resolver"),
	}
}

// Resolve dispatches secret resolution based on reference provider or default.
func (r *CompositeSecretResolver) Resolve(ctx context.Context, secretRef string) (string, error) {
	ctx, span := r.tracer.Start(ctx, "CompositeSecretResolver.Resolve")
	defer span.End()

	parsed := ParseSecretRef(secretRef)

	provider := parsed.Provider
	if provider == "" {
		if !strings.Contains(secretRef, "/") {
			// Plaintext literal fallback
			return secretRef, nil
		}
		provider = r.defaultProvider
	}

	if provider == "" {
		return "", fmt.Errorf("secret provider is not specified for ref %q and no default provider is configured in secrets_config", secretRef)
	}

	span.SetAttributes(attribute.String("secrets.resolved_provider", provider))

	switch provider {
	case "vault", "openbao", "bao":
		if r.openbao == nil {
			return "", fmt.Errorf("openbao secret resolver not configured for ref %q", secretRef)
		}
		return r.openbao.Resolve(ctx, secretRef)

	case "gcp", "gcp_sm", "gcp_secret_manager":
		if r.gcp == nil {
			return "", fmt.Errorf("gcp secret manager resolver not configured for ref %q", secretRef)
		}
		return r.gcp.Resolve(ctx, secretRef)

	default:
		return "", fmt.Errorf("unsupported secret provider %q for ref %q", provider, secretRef)
	}
}
