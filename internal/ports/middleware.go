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

// WriterMiddleware wraps an io.WriteCloser with an output-side stream transformation.
// Implementations must ensure that Close() finalizes any trailers (e.g. PGP armor footer,
// GCM authentication tag) before the underlying writer is closed.
type WriterMiddleware func(w io.WriteCloser) (io.WriteCloser, error)

// StreamEncryptor defines the contract for streaming encryption on the write path
// (e.g. PGP public-key encryption, GCP KMS envelope encryption).
// EncryptStream returns a new io.WriteCloser that encrypts data written to it
// and writes the ciphertext to w. Closing the returned writer must flush and
// finalize the encryption before the underlying w is finalized.
type StreamEncryptor interface {
	EncryptStream(ctx context.Context, w io.Writer, keyRef string, sec SecretResolver) (io.WriteCloser, error)
}
