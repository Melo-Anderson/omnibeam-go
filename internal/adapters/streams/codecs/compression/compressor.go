package compression

import (
	"compress/gzip"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

var gzipWriterPool = sync.Pool{
	New: func() any {
		return gzip.NewWriter(io.Discard)
	},
}

type pooledGzipWriterCloser struct {
	w *gzip.Writer
}

func (p *pooledGzipWriterCloser) Close() error {
	err := p.w.Close()
	p.w.Reset(io.Discard)
	gzipWriterPool.Put(p.w)
	return err
}

var zstdWriterPool = sync.Pool{
	New: func() any {
		enc, _ := zstd.NewWriter(io.Discard, zstd.WithEncoderConcurrency(1))
		return enc
	},
}

type pooledZstdWriterCloser struct {
	enc *zstd.Encoder
}

func (p *pooledZstdWriterCloser) Close() error {
	err := p.enc.Close()
	p.enc.Reset(io.Discard)
	zstdWriterPool.Put(p.enc)
	return err
}

var snappyWriterPool = sync.Pool{
	New: func() any {
		return snappy.NewBufferedWriter(io.Discard)
	},
}

type pooledSnappyWriterCloser struct {
	w *snappy.Writer
}

func (p *pooledSnappyWriterCloser) Close() error {
	err := p.w.Close()
	p.w.Reset(io.Discard)
	snappyWriterPool.Put(p.w)
	return err
}

// WrapCompressor wraps w with the requested compression codec.
// Returns the wrapped writer, a Closer to flush and finalize (may be nil for "none" or empty),
// and any initialization error.
func WrapCompressor(w io.Writer, compression string) (io.Writer, io.Closer, error) {
	switch strings.ToLower(strings.TrimSpace(compression)) {
	case "", "none":
		return w, nil, nil
	case "gzip", "gz":
		gw := gzipWriterPool.Get().(*gzip.Writer)
		gw.Reset(w)
		return gw, &pooledGzipWriterCloser{w: gw}, nil
	case "zstd":
		enc := zstdWriterPool.Get().(*zstd.Encoder)
		enc.Reset(w)
		return enc, &pooledZstdWriterCloser{enc: enc}, nil
	case "snappy", "sz":
		sw := snappyWriterPool.Get().(*snappy.Writer)
		sw.Reset(w)
		return sw, &pooledSnappyWriterCloser{w: sw}, nil
	default:
		return nil, nil, fmt.Errorf("unsupported output compression format: %q (supported: gzip, zstd, snappy, none)", compression)
	}
}

