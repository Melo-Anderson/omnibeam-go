package charsets_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/charsets"
)

func TestWrapNormalizer_AllCharsets(t *testing.T) {
	t.Run("ISO-8859-1 / Latin-1", func(t *testing.T) {
		latin1Bytes := []byte{0x53, 0xE3, 0x6F, 0x20, 0x50, 0x61, 0x75, 0x6C, 0x6F} // "São Paulo"
		for _, enc := range []string{"iso-8859-1", "latin1", "latin-1", "LATIN1"} {
			reader, err := charsets.WrapNormalizer(bytes.NewReader(latin1Bytes), enc)
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", enc, err)
			}
			utf8Bytes, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("failed reading: %v", err)
			}
			if string(utf8Bytes) != "São Paulo" {
				t.Errorf("expected São Paulo, got %s", string(utf8Bytes))
			}
		}
	})

	t.Run("Windows-1252 / CP1252", func(t *testing.T) {
		cp1252Bytes := []byte{0x80, 0x20, 0x54, 0x65, 0x73, 0x74} // € Test in CP1252 (0x80 = €)
		for _, enc := range []string{"windows-1252", "cp1252"} {
			reader, err := charsets.WrapNormalizer(bytes.NewReader(cp1252Bytes), enc)
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", enc, err)
			}
			res, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("failed reading: %v", err)
			}
			if string(res) != "€ Test" {
				t.Errorf("expected € Test, got %s", string(res))
			}
		}
	})

	t.Run("UTF-16LE and UTF-16BE", func(t *testing.T) {
		// UTF-16LE: "AB" = 0x41, 0x00, 0x42, 0x00
		leBytes := []byte{0x41, 0x00, 0x42, 0x00}
		readerLE, err := charsets.WrapNormalizer(bytes.NewReader(leBytes), "utf-16le")
		if err != nil {
			t.Fatalf("unexpected error for utf-16le: %v", err)
		}
		resLE, _ := io.ReadAll(readerLE)
		if string(resLE) != "AB" {
			t.Errorf("expected AB, got %s", string(resLE))
		}

		// UTF-16BE: "AB" = 0x00, 0x41, 0x00, 0x42
		beBytes := []byte{0x00, 0x41, 0x00, 0x42}
		readerBE, err := charsets.WrapNormalizer(bytes.NewReader(beBytes), "utf-16be")
		if err != nil {
			t.Fatalf("unexpected error for utf-16be: %v", err)
		}
		resBE, _ := io.ReadAll(readerBE)
		if string(resBE) != "AB" {
			t.Errorf("expected AB, got %s", string(resBE))
		}
	})

	t.Run("UTF-8 and empty", func(t *testing.T) {
		orig := "Olá mundo UTF-8"
		for _, enc := range []string{"", "utf-8", "utf8", "UTF-8"} {
			reader, err := charsets.WrapNormalizer(bytes.NewReader([]byte(orig)), enc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			res, _ := io.ReadAll(reader)
			if string(res) != orig {
				t.Errorf("expected %q, got %q", orig, string(res))
			}
		}
	})

	t.Run("Unsupported charset", func(t *testing.T) {
		_, err := charsets.WrapNormalizer(bytes.NewReader([]byte("test")), "unsupported-ebcdic")
		if err == nil {
			t.Error("expected error for unsupported charset, got nil")
		}
	})
}
