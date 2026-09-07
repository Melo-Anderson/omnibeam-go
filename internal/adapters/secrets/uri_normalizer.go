// Package secrets implements enterprise secret management and credential resolution.
package secrets

import (
	"context"
	"fmt"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// ParsedSecretRef contains components of a parsed universal secret reference.
type ParsedSecretRef struct {
	Provider string
	Path     string
	Field    string
}

// ParseSecretRef parses a universal secret reference in the form [provider:]mount/path[#field].
func ParseSecretRef(ref string) ParsedSecretRef {
	trimmed := strings.TrimSpace(ref)
	var provider, remainder string

	if idx := strings.Index(trimmed, ":"); idx != -1 && !strings.Contains(trimmed[:idx], "/") {
		provider = strings.ToLower(trimmed[:idx])
		remainder = trimmed[idx+1:]
	} else {
		remainder = trimmed
	}

	var path, field string
	if hashIdx := strings.Index(remainder, "#"); hashIdx != -1 {
		path = remainder[:hashIdx]
		field = remainder[hashIdx+1:]
	} else {
		path = remainder
	}

	return ParsedSecretRef{
		Provider: provider,
		Path:     path,
		Field:    field,
	}
}

// NormalizeVaultPath ensures KV v2 paths have the /data/ segment after the mount prefix.
func NormalizeVaultPath(path string) (string, bool) {
	trimmed := strings.Trim(strings.TrimSpace(path), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 {
		return path, false
	}

	mount := parts[0]
	subpath := strings.Join(parts[1:], "/")

	if strings.HasPrefix(subpath, "data/") || subpath == "data" {
		return trimmed, false
	}

	return fmt.Sprintf("%s/data/%s", mount, subpath), true
}

// ApplyHostOverride replaces the "host" field value in the payload if mapped in overrides.
func ApplyHostOverride(payload map[string]any, overrides map[string]string) {
	if len(overrides) == 0 {
		return
	}
	if hostVal, ok := payload["host"].(string); ok {
		if replacement, exists := overrides[hostVal]; exists {
			payload["host"] = replacement
		}
	}
}

// ApplyDatabaseHostOverrides rewrites database connection URI and host based on host override mappings.
func ApplyDatabaseHostOverrides(cfg *domain.DatabaseSourceConfig, overrides map[string]string) {
	if cfg == nil || len(overrides) == 0 {
		return
	}
	for target, replacement := range overrides {
		atTarget := "@" + target
		atReplacement := "@" + replacement
		if strings.Contains(cfg.ConnectionURI, atTarget) {
			cfg.ConnectionURI = strings.ReplaceAll(cfg.ConnectionURI, atTarget, atReplacement)
		}
		tcpTarget := "(" + target
		tcpReplacement := "(" + replacement
		if strings.Contains(cfg.ConnectionURI, tcpTarget) {
			cfg.ConnectionURI = strings.ReplaceAll(cfg.ConnectionURI, tcpTarget, tcpReplacement)
		}
		if cfg.Host == target {
			cfg.Host = replacement
		}
	}
}

// ResolveConnectionURI resolves passwords and full-URI secret references for database connections.
func ResolveConnectionURI(ctx context.Context, uri, passwordRef string, resolver ports.SecretResolver) (string, error) {
	resolvedURI := strings.TrimSpace(uri)
	if resolvedURI == "" {
		return "", fmt.Errorf("connection_uri is required")
	}

	var err error
	resolvedURI, err = interpolatePassword(ctx, resolvedURI, passwordRef, resolver)
	if err != nil {
		return "", err
	}

	return resolveURISecret(ctx, resolvedURI, resolver)
}

func interpolatePassword(ctx context.Context, uri, passwordRef string, resolver ports.SecretResolver) (string, error) {
	if passwordRef == "" || resolver == nil {
		return uri, nil
	}
	pwd, err := resolver.Resolve(ctx, passwordRef)
	if err != nil {
		return "", fmt.Errorf("failed resolving connection password: %w", err)
	}

	result := uri
	if strings.Contains(result, "{{password}}") {
		result = strings.ReplaceAll(result, "{{password}}", pwd)
	}
	if strings.Contains(result, "{{PASSWORD}}") {
		result = strings.ReplaceAll(result, "{{PASSWORD}}", pwd)
	}
	return result, nil
}

func resolveURISecret(ctx context.Context, uri string, resolver ports.SecretResolver) (string, error) {
	if resolver == nil {
		return uri, nil
	}
	if strings.HasPrefix(uri, "gcp:") || strings.HasPrefix(uri, "openbao:") || strings.HasPrefix(uri, "vault:") {
		resolved, err := resolver.Resolve(ctx, uri)
		if err != nil {
			return "", fmt.Errorf("failed resolving connection uri from secret manager: %w", err)
		}
		if resolved != "" {
			return resolved, nil
		}
	}
	return uri, nil
}

// ResolveAPIAuth resolves credentials for HTTP API authentication configuration into a secrets lookup map.
func ResolveAPIAuth(ctx context.Context, auth *domain.APIAuthConfig, resolver ports.SecretResolver) map[string]string {
	secretsMap := make(map[string]string)
	if auth == nil || resolver == nil {
		return secretsMap
	}

	resolveSecretRef(ctx, auth.TokenRef, resolver, secretsMap)
	resolveSecretRef(ctx, auth.UsernameRef, resolver, secretsMap)
	resolveSecretRef(ctx, auth.PasswordRef, resolver, secretsMap)
	resolveSecretRef(ctx, auth.ClientIDRef, resolver, secretsMap)
	resolveSecretRef(ctx, auth.ClientSecretRef, resolver, secretsMap)

	return secretsMap
}

func resolveSecretRef(ctx context.Context, ref string, resolver ports.SecretResolver, out map[string]string) {
	if strings.TrimSpace(ref) == "" {
		return
	}
	if val, err := resolver.Resolve(ctx, ref); err == nil && val != "" {
		out[ref] = val
	}
}
