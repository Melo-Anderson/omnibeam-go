// package codecs implements the composable stream processing pipeline
// including decompression, charset normalization, and format tokenizers.
package codecs

import (
	"context"
	"fmt"
	"io"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/charsets"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/compression"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/parsers"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var _ ports.StreamWrapper = (*PipelineBuilder)(nil)

type PipelineBuilder struct {
	decryptor      ports.StreamDecryptor
	keyRef         string
	secretResolver ports.SecretResolver
}

func NewPipelineBuilder() *PipelineBuilder {
	return &PipelineBuilder{}
}

// WithDecryptor sets an optional decryptor for encrypted storage streams.
func (b *PipelineBuilder) WithDecryptor(decryptor ports.StreamDecryptor, keyRef string, sec ports.SecretResolver) *PipelineBuilder {
	b.decryptor = decryptor
	b.keyRef = keyRef
	b.secretResolver = sec
	return b
}

func (b *PipelineBuilder) WrapStream(r io.Reader, srcCfg *domain.SourceConfig) (io.Reader, error) {
	current := r
	if b.decryptor != nil && b.keyRef != "" {
		decrypted, err := b.decryptor.DecryptStream(context.Background(), current, b.keyRef, b.secretResolver)
		if err != nil {
			return nil, fmt.Errorf("failed to apply stream decryption: %w", err)
		}
		current = decrypted
	}

	decompressed, err := compression.WrapDecompressor(current, srcCfg.Compression)
	if err != nil {
		return nil, fmt.Errorf("failed to apply decompression: %w", err)
	}

	normalized, err := charsets.WrapNormalizer(decompressed, srcCfg.Charset)
	if err != nil {
		return nil, fmt.Errorf("failed to apply charset normalizer: %w", err)
	}

	return normalized, nil
}

func (b *PipelineBuilder) BuildDecoder(srcCfg *domain.SourceConfig) (ports.StreamDecoder, error) {
	format := ""
	if srcCfg != nil {
		format = srcCfg.Format
	}
	return parsers.Build(format, srcCfg)
}
