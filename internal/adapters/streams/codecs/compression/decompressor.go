package compression

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

var gzipReaderPool = sync.Pool{
	New: func() any {
		return new(gzip.Reader)
	},
}

type pooledGzipReader struct {
	r *gzip.Reader
}

func (p *pooledGzipReader) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

// Close closes the gzip.Reader and returns it to the pool for reuse.
func (p *pooledGzipReader) Close() error {
	err := p.r.Close()
	gzipReaderPool.Put(p.r)
	return err
}

var zstdDecoderPool = sync.Pool{
	New: func() any {
		dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		return dec
	},
}

type pooledZstdReader struct {
	dec *zstd.Decoder
}

func (p *pooledZstdReader) Read(b []byte) (int, error) {
	return p.dec.Read(b)
}

// Close resets the zstd.Decoder and returns it to the pool for reuse.
func (p *pooledZstdReader) Close() error {
	_ = p.dec.Reset(nil)
	zstdDecoderPool.Put(p.dec)
	return nil
}

var snappyReaderPool = sync.Pool{
	New: func() any {
		return snappy.NewReader(nil)
	},
}

type pooledSnappyReader struct {
	r *snappy.Reader
}

func (p *pooledSnappyReader) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

// Close resets the snappy.Reader and returns it to the pool for reuse.
func (p *pooledSnappyReader) Close() error {
	p.r.Reset(nil)
	snappyReaderPool.Put(p.r)
	return nil
}

// WrapDecompressor wraps r in the appropriate decompressor for compType.
// Readers for gzip, zstd, and snappy are pooled via sync.Pool for bundle-level reuse.
func WrapDecompressor(r io.Reader, compType string) (io.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(compType)) {
	case "", "none":
		return r, nil
	case "gzip", "gz":
		gr := gzipReaderPool.Get().(*gzip.Reader)
		if err := gr.Reset(r); err != nil {
			gzipReaderPool.Put(gr)
			return nil, fmt.Errorf("failed to reset gzip reader: %w", err)
		}
		return &pooledGzipReader{r: gr}, nil
	case "zstd":
		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		if err := dec.Reset(r); err != nil {
			zstdDecoderPool.Put(dec)
			return nil, fmt.Errorf("failed to reset zstd reader: %w", err)
		}
		return &pooledZstdReader{dec: dec}, nil
	case "snappy", "sz":
		sr := snappyReaderPool.Get().(*snappy.Reader)
		sr.Reset(r)
		return &pooledSnappyReader{r: sr}, nil
	case "bzip2", "bz2":
		return bzip2.NewReader(r), nil
	default:
		return nil, fmt.Errorf("unsupported compression format: %q (supported: gzip, zstd, snappy, bzip2, none)", compType)
	}
}
