package ioutils

import (
	"context"
	"fmt"
	"io"
)

// CloserChain closes multiple io.Closers in sequential order,
// ensuring every closer is attempted and returning the first error encountered.
type CloserChain []io.Closer

// Close iterates through each closer in the chain and closes it.
func (c CloserChain) Close() error {
	var firstErr error
	for _, cl := range c {
		if cl != nil {
			if err := cl.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// DecoratedStreamWriter wraps an underlying writer with optional SHA-256 hash calculation,
// stream encryption, and compression codecs.
type DecoratedStreamWriter struct {
	Writer  io.Writer
	HashW   *CountingHashWriter
	closers CloserChain
}

// Write writes bytes to the active top-level writer.
func (d *DecoratedStreamWriter) Write(p []byte) (int, error) {
	return d.Writer.Write(p)
}

// Close flushes and closes all wrapped compression and encryption layers in correct sequence.
func (d *DecoratedStreamWriter) Close() error {
	return d.closers.Close()
}

// HexSum returns the SHA-256 hex string of the underlying raw storage writer if hash is enabled.
func (d *DecoratedStreamWriter) HexSum() string {
	if d.HashW != nil {
		return d.HashW.HexSum()
	}
	return ""
}

// BytesWritten returns the total raw bytes written to the underlying storage writer.
func (d *DecoratedStreamWriter) BytesWritten() int64 {
	if d.HashW != nil {
		return d.HashW.Written()
	}
	return 0
}

// StreamPipelineOptions configures optional encryption and compression decorators.
type StreamPipelineOptions struct {
	Compression    string
	EncryptionType string
}

// EncryptorProviderFunc is a function that wraps an io.Writer with a stream encryptor.
type EncryptorProviderFunc func(ctx context.Context, w io.Writer) (io.WriteCloser, error)

// CompressorProviderFunc is a function that wraps an io.Writer with a stream compressor.
type CompressorProviderFunc func(w io.Writer, compression string) (io.Writer, io.Closer, error)

// BuildDecoratedStreamWriter builds the pipeline: Storage -> Hash (SHA-256) -> Encryption -> Compression.
func BuildDecoratedStreamWriter(
	ctx context.Context,
	rawWriter io.Writer,
	opts StreamPipelineOptions,
	encryptorFn EncryptorProviderFunc,
	compressorFn CompressorProviderFunc,
) (*DecoratedStreamWriter, error) {
	hashW := NewCountingHashWriter(rawWriter)
	targetDest := io.Writer(hashW)

	var encCloser io.Closer
	if opts.EncryptionType != "" && opts.EncryptionType != "none" {
		if encryptorFn == nil {
			return nil, fmt.Errorf("stream encryptor not configured for encryption type %q", opts.EncryptionType)
		}
		encWriter, err := encryptorFn(ctx, targetDest)
		if err != nil {
			return nil, fmt.Errorf("failed initializing stream encryption: %w", err)
		}
		encCloser = encWriter
		targetDest = encWriter
	}

	var compCloser io.Closer
	if compressorFn != nil && opts.Compression != "" && opts.Compression != "none" {
		compWriter, closer, err := compressorFn(targetDest, opts.Compression)
		if err != nil {
			if encCloser != nil {
				_ = encCloser.Close()
			}
			return nil, fmt.Errorf("failed initializing stream compression: %w", err)
		}
		compCloser = closer
		targetDest = compWriter
	}

	return &DecoratedStreamWriter{
		Writer:  targetDest,
		HashW:   hashW,
		closers: CloserChain{compCloser, encCloser},
	}, nil
}
