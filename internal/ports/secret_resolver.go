// Package ports defines domain-level abstract contracts and interfaces
// for storage, streaming, codecs, database readers, and secret resolvers.
package ports

import "context"

// SecretResolver abstracts dynamic retrieval of plaintext credentials
// from environment variables, key vaults, or secret managers.
type SecretResolver interface {
	Resolve(ctx context.Context, secretRef string) (string, error)
}
