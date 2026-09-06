package formatters_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/formatters"
)

func TestBufferPool_GetAndPut(t *testing.T) {
	buf := formatters.GetBuffer()
	if buf == nil {
		t.Fatal("expected non-nil buffer from GetBuffer")
	}
	buf.WriteString("test data for pooling")
	if buf.Len() == 0 {
		t.Errorf("buffer should contain written data")
	}
	formatters.PutBuffer(buf)

	buf2 := formatters.GetBuffer()
	if buf2.Len() != 0 {
		t.Errorf("expected reset buffer from pool, got len %d", buf2.Len())
	}
	formatters.PutBuffer(buf2)
}
