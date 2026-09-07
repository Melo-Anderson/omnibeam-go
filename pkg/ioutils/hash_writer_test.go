package ioutils_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/pkg/ioutils"
)

func TestCountingHashWriter(t *testing.T) {
	var buf bytes.Buffer
	hw := ioutils.NewCountingHashWriter(&buf)

	data := []byte("hello, omnibeam distributed compute!")
	n, err := hw.Write(data)
	if err != nil || n != len(data) {
		t.Fatalf("Write failed: n=%d, err=%v", n, err)
	}

	if hw.Written() != int64(len(data)) {
		t.Errorf("expected %d written bytes, got %d", len(data), hw.Written())
	}

	expectedHash := sha256.Sum256(data)
	expectedHex := hex.EncodeToString(expectedHash[:])

	if hw.HexSum() != expectedHex {
		t.Errorf("expected hash hex %s, got %s", expectedHex, hw.HexSum())
	}

	if !bytes.Equal(hw.Sum(), expectedHash[:]) {
		t.Errorf("Sum() mismatch: got %x, want %x", hw.Sum(), expectedHash)
	}
}
