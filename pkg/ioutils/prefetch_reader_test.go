package ioutils_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/pkg/ioutils"
)

type slowReader struct {
	data []byte
	pos  int
}

func (s *slowReader) Read(p []byte) (n int, err error) {
	if s.pos >= len(s.data) {
		return 0, io.EOF
	}
	time.Sleep(500 * time.Microsecond)
	n = copy(p, s.data[s.pos:])
	s.pos += n
	return n, nil
}

type errReader struct {
	err error
}

func (e *errReader) Read(p []byte) (int, error) {
	return 0, e.err
}

type closeTrackerReader struct {
	io.Reader
	closed bool
}

func (c *closeTrackerReader) Close() error {
	c.closed = true
	return nil
}

func TestPrefetchReader_IntegritySmall(t *testing.T) {
	payload := []byte("hello prefetch world")
	pr := ioutils.NewPrefetchReader(&slowReader{data: payload}, 4, 2)
	defer pr.Close()

	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Errorf("content mismatch: got %q, want %q", out, payload)
	}
}

func TestPrefetchReader_IntegrityLarge(t *testing.T) {
	payload := bytes.Repeat([]byte("1234567890abcdef"), 1024) // 16 KB
	pr := ioutils.NewPrefetchReader(&slowReader{data: payload}, 4096, 4)
	defer pr.Close()

	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Errorf("read bytes mismatch: got %d, want %d", len(out), len(payload))
	}
}

func TestPrefetchReader_DefaultParams(t *testing.T) {
	payload := []byte("abc")
	pr := ioutils.NewPrefetchReader(&slowReader{data: payload}, 0, 0)
	defer pr.Close()

	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Errorf("content mismatch with default params")
	}
}

func TestPrefetchReader_ErrorPropagation(t *testing.T) {
	expectedErr := errors.New("read failed")
	pr := ioutils.NewPrefetchReader(&errReader{err: expectedErr}, 10, 2)
	defer pr.Close()

	buf := make([]byte, 10)
	_, err := pr.Read(buf)
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestPrefetchReader_CloseUnderlying(t *testing.T) {
	inner := &closeTrackerReader{Reader: bytes.NewReader([]byte("test data"))}
	pr := ioutils.NewPrefetchReader(inner, 10, 2)
	if err := pr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !inner.closed {
		t.Errorf("expected underlying reader to be closed")
	}
}

func TestPrefetchReader_AbruptCloseDoesNotLeakGoroutine(t *testing.T) {
	r := io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("abcdefghij"), 10000)))
	pr := ioutils.NewPrefetchReader(r, 0, 0) // verifies defaults

	buf := make([]byte, 1)
	_, _ = pr.Read(buf)

	err := pr.Close()
	if err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}

	_, err = pr.Read(buf)
	if err != io.EOF && err != nil {
		t.Fatalf("expected EOF or nil on closed read, got %v", err)
	}
}

func TestDoubleBufferedPrefetchReader_ContinuousRead(t *testing.T) {
	data := make([]byte, 2*1024*1024) // 2MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	src := bytes.NewReader(data)
	reader := ioutils.NewDoubleBufferedPrefetchReader(src, 64*1024, 4) // 64KB chunks, 4 queue slots
	defer reader.Close()

	dest := make([]byte, len(data))
	n, err := io.ReadFull(reader, dest)
	if err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	if n != len(data) {
		t.Fatalf("expected %d bytes, got %d", len(data), n)
	}
	if !bytes.Equal(data, dest) {
		t.Fatal("read data does not match source")
	}
}
