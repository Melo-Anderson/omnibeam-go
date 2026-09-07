// Package ioutils provides optimized I/O helpers and async stream wrappers.
package ioutils

import (
	"errors"
	"io"
	"sync"
)

const (
	// DefaultPrefetchChunkSize defines the default read buffer chunk size (64 KiB).
	DefaultPrefetchChunkSize = 64 * 1024
	// DefaultPrefetchBufferCount defines the default number of chunks queued in memory.
	DefaultPrefetchBufferCount = 2
)

type prefetchChunk struct {
	data []byte
	err  error
}

// PrefetchReader buffers reads from an underlying io.Reader in a background goroutine.
// This decouples I/O wait from the caller's processing loop.
type PrefetchReader struct {
	underlying io.Reader
	chunksCh   chan prefetchChunk
	current    []byte
	pos        int
	closedCh   chan struct{}
	closeOnce  sync.Once
	readErr    error
}

// NewPrefetchReader constructs an async double-buffered reader using DefaultPrefetchChunkSize and DefaultPrefetchBufferCount.
func NewPrefetchReader(r io.Reader, chunkSize int, bufferCount int) io.ReadCloser {
	if chunkSize <= 0 {
		chunkSize = DefaultPrefetchChunkSize
	}
	if bufferCount <= 0 {
		bufferCount = DefaultPrefetchBufferCount
	}
	pr := &PrefetchReader{
		underlying: r,
		chunksCh:   make(chan prefetchChunk, bufferCount),
		closedCh:   make(chan struct{}),
	}
	go pr.prefetchLoop(chunkSize)
	return pr
}

// NewDoubleBufferedPrefetchReader creates a double-buffered prefetch reader with specified chunk size and queue depth.
func NewDoubleBufferedPrefetchReader(r io.Reader, chunkSize int, queueDepth int) io.ReadCloser {
	return NewPrefetchReader(r, chunkSize, queueDepth)
}

func (pr *PrefetchReader) prefetchLoop(chunkSize int) {
	defer close(pr.chunksCh)
	for {
		buf := make([]byte, chunkSize)
		n, err := pr.underlying.Read(buf)
		if n > 0 {
			if !pr.sendChunk(prefetchChunk{data: buf[:n]}) {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				_ = pr.sendChunk(prefetchChunk{err: err})
			}
			return
		}
	}
}

func (pr *PrefetchReader) sendChunk(chunk prefetchChunk) bool {
	select {
	case <-pr.closedCh:
		return false
	case pr.chunksCh <- chunk:
		return true
	}
}

// Read satisfies io.Reader from the prefetch buffer queue.
func (pr *PrefetchReader) Read(p []byte) (int, error) {
	select {
	case <-pr.closedCh:
		return 0, io.EOF
	default:
	}
	if pr.pos < len(pr.current) {
		n := copy(p, pr.current[pr.pos:])
		pr.pos += n
		return n, nil
	}
	if pr.readErr != nil {
		return 0, pr.readErr
	}
	return pr.readNextChunk(p)
}

func (pr *PrefetchReader) readNextChunk(p []byte) (int, error) {
	select {
	case <-pr.closedCh:
		return 0, io.EOF
	case c, ok := <-pr.chunksCh:
		if !ok {
			return 0, io.EOF
		}
		if c.err != nil {
			pr.readErr = c.err
			return 0, c.err
		}
		pr.current = c.data
		pr.pos = copy(p, pr.current)
		return pr.pos, nil
	}
}

// Close signals the prefetch goroutine to stop and closes the underlying reader if it implements io.Closer.
func (pr *PrefetchReader) Close() error {
	pr.closeOnce.Do(func() { close(pr.closedCh) })
	if c, ok := pr.underlying.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
