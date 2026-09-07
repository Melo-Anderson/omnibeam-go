package ioutils

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

// CountingHashWriter wraps an io.Writer to count bytes written and compute SHA-256 on the fly.
type CountingHashWriter struct {
	underlying io.Writer
	hasher     hash.Hash
	written    int64
}

// NewCountingHashWriter constructs a new CountingHashWriter wrapping the target writer.
func NewCountingHashWriter(underlying io.Writer) *CountingHashWriter {
	return &CountingHashWriter{
		underlying: underlying,
		hasher:     sha256.New(),
	}
}

// Write writes bytes to the underlying writer and feeds the hash and byte counter.
func (c *CountingHashWriter) Write(p []byte) (n int, err error) {
	n, err = c.underlying.Write(p)
	if n > 0 {
		c.written += int64(n)
		c.hasher.Write(p[:n])
	}
	return n, err
}

// Written returns the total number of bytes written so far.
func (c *CountingHashWriter) Written() int64 {
	return c.written
}

// Sum returns the SHA-256 hash digest.
func (c *CountingHashWriter) Sum() []byte {
	return c.hasher.Sum(nil)
}

// HexSum returns the hexadecimal encoded SHA-256 hash digest.
func (c *CountingHashWriter) HexSum() string {
	return hex.EncodeToString(c.Sum())
}
