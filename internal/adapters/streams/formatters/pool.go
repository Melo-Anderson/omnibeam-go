package formatters

import (
	"bytes"
	"sync"
)

const defaultPoolBufferSize = 32 * 1024 // 32 KB

var bufferPool = sync.Pool{
	New: func() any {
		b := bytes.NewBuffer(make([]byte, 0, defaultPoolBufferSize))
		return b
	},
}

// GetBuffer retrieves a cleared bytes.Buffer from the pool.
func GetBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// PutBuffer resets and returns the buffer to the pool if not excessively oversized.
// Buffers larger than 1 MB are discarded to prevent unbounded memory retention.
func PutBuffer(buf *bytes.Buffer) {
	if buf == nil || buf.Cap() > 1024*1024 {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}
