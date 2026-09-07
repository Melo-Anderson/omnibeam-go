// Package ports defines domain-level abstract contracts and interfaces
// for storage, streaming, codecs, and sinks.
package ports

import (
	"context"
	"io"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

type ReaderMiddleware func(r io.Reader) (io.Reader, error)

type StreamWrapper interface {
	WrapStream(r io.Reader, cfg *domain.SourceConfig) (io.Reader, error)
}

// StreamDecryptor defines the contract for streaming data decryption (e.g. GCP KMS, PGP, AES-GCM).
type StreamDecryptor interface {
	DecryptStream(ctx context.Context, r io.Reader, keyRef string, sec SecretResolver) (io.Reader, error)
}
