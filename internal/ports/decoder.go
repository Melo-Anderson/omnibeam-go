// Package ports defines domain-level abstract contracts and interfaces
// for storage, streaming, codecs, and sinks.
package ports

import (
	"context"
	"io"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

type StreamDecoder interface {
	Decode(ctx context.Context, r io.Reader, schema *domain.Schema, sourceFile string) (<-chan *domain.GenericRecord, <-chan error, error)
}
