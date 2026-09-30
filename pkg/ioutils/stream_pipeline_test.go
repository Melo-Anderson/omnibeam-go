package ioutils

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type dummyWriteCloser struct {
	io.Writer
	closed bool
}

func (d *dummyWriteCloser) Close() error {
	d.closed = true
	return nil
}

type errCloser struct {
	io.Writer
	err error
}

func (e *errCloser) Close() error {
	return e.err
}

func TestCloserChain(t *testing.T) {
	c1 := &dummyWriteCloser{Writer: &bytes.Buffer{}}
	c2 := &dummyWriteCloser{Writer: &bytes.Buffer{}}

	chain := CloserChain{c1, c2, nil}
	if err := chain.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !c1.closed || !c2.closed {
		t.Errorf("expected both closers to be closed: c1=%v, c2=%v", c1.closed, c2.closed)
	}

	errExpected := errors.New("close failed")
	c3 := &errCloser{err: errExpected}
	c4 := &dummyWriteCloser{}
	errChain := CloserChain{c3, c4}
	err := errChain.Close()
	if !errors.Is(err, errExpected) {
		t.Errorf("expected error %v, got %v", errExpected, err)
	}
	if !c4.closed {
		t.Errorf("expected remaining closer to still be closed despite previous error")
	}
}

func TestBuildDecoratedStreamWriter_Passthrough(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()
	opts := StreamPipelineOptions{}

	d, err := BuildDecoratedStreamWriter(ctx, &buf, opts, nil, nil)
	if err != nil {
		t.Fatalf("BuildDecoratedStreamWriter failed: %v", err)
	}

	data := []byte("hello world")
	n, err := d.Write(data)
	if err != nil || n != len(data) {
		t.Fatalf("Write failed: n=%d, err=%v", n, err)
	}

	if buf.String() != "hello world" {
		t.Errorf("expected 'hello world', got %q", buf.String())
	}
	if d.BytesWritten() != int64(len(data)) {
		t.Errorf("expected %d bytes written, got %d", len(data), d.BytesWritten())
	}
	if d.HexSum() == "" {
		t.Errorf("expected non-empty HexSum")
	}

	if err := d.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestBuildDecoratedStreamWriter_WithEncryptionAndCompression(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()
	opts := StreamPipelineOptions{
		Compression:    "gzip",
		EncryptionType: "pgp",
	}

	encCloser := &dummyWriteCloser{Writer: &buf}
	compCloser := &dummyWriteCloser{Writer: encCloser}

	encCalled := false
	compCalled := false

	encFn := func(_ context.Context, w io.Writer) (io.WriteCloser, error) {
		encCalled = true
		return encCloser, nil
	}

	compFn := func(w io.Writer, comp string) (io.Writer, io.Closer, error) {
		compCalled = true
		if comp != "gzip" {
			t.Errorf("unexpected comp %q", comp)
		}
		return compCloser, compCloser, nil
	}

	d, err := BuildDecoratedStreamWriter(ctx, &buf, opts, encFn, compFn)
	if err != nil {
		t.Fatalf("BuildDecoratedStreamWriter failed: %v", err)
	}

	if !encCalled || !compCalled {
		t.Errorf("expected both providers called: enc=%v, comp=%v", encCalled, compCalled)
	}

	_, _ = d.Write([]byte("encrypted data"))
	if err := d.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if !compCloser.closed || !encCloser.closed {
		t.Errorf("expected both wrappers closed: comp=%v, enc=%v", compCloser.closed, encCloser.closed)
	}
}

func TestBuildDecoratedStreamWriter_Failures(t *testing.T) {
	ctx := context.Background()
	opts := StreamPipelineOptions{EncryptionType: "pgp"}

	// 1. Missing encryptor
	_, err := BuildDecoratedStreamWriter(ctx, &bytes.Buffer{}, opts, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "stream encryptor not configured") {
		t.Errorf("expected missing encryptor error, got %v", err)
	}

	// 2. Encryptor error
	errFn := func(_ context.Context, _ io.Writer) (io.WriteCloser, error) {
		return nil, errors.New("kms error")
	}
	_, err = BuildDecoratedStreamWriter(ctx, &bytes.Buffer{}, opts, errFn, nil)
	if err == nil || !strings.Contains(err.Error(), "kms error") {
		t.Errorf("expected kms error, got %v", err)
	}

	// 3. Compressor error closes opened encryption writer
	encCloser := &dummyWriteCloser{Writer: &bytes.Buffer{}}
	optsWithComp := StreamPipelineOptions{EncryptionType: "pgp", Compression: "zstd"}
	goodEncFn := func(_ context.Context, _ io.Writer) (io.WriteCloser, error) {
		return encCloser, nil
	}
	errCompFn := func(_ io.Writer, _ string) (io.Writer, io.Closer, error) {
		return nil, nil, errors.New("zstd error")
	}

	_, err = BuildDecoratedStreamWriter(ctx, &bytes.Buffer{}, optsWithComp, goodEncFn, errCompFn)
	if err == nil || !strings.Contains(err.Error(), "zstd error") {
		t.Errorf("expected zstd error, got %v", err)
	}
	if !encCloser.closed {
		t.Errorf("expected encCloser to be aborted/closed on compression failure")
	}
}
