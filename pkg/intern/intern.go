// Package intern provides concurrency-safe string interning with bounded capacity
// and length thresholds to prevent memory exhaustion (OOM) on high-cardinality data.
package intern

import (
	"sync"
	"unsafe"
)

const (
	// DefaultMaxInternStringLen defines the maximum byte length of strings eligible for interning.
	DefaultMaxInternStringLen = 64
	// DefaultMaxInternPoolSize defines the maximum number of entries allowed in the global pool.
	DefaultMaxInternPoolSize = 16384
	// DefaultInitialPoolCapacity defines the initial allocation capacity of the pool map.
	DefaultInitialPoolCapacity = 1024
)

type Pool struct {
	mu      sync.RWMutex
	entries map[string]string
}

var globalPool = &Pool{
	entries: make(map[string]string, DefaultInitialPoolCapacity),
}

// ResetForTesting clears the internal pool (only used in test suites).
func ResetForTesting() {
	globalPool.mu.Lock()
	defer globalPool.mu.Unlock()
	globalPool.entries = make(map[string]string, DefaultInitialPoolCapacity)
}

// String returns a canonical shared string instance for strings <= DefaultMaxInternStringLen.
// Strings exceeding length or when the pool is saturated are returned without allocation.
func String(s string) string {
	if len(s) == 0 {
		return ""
	}
	if len(s) > DefaultMaxInternStringLen {
		return s
	}

	globalPool.mu.RLock()
	if v, ok := globalPool.entries[s]; ok {
		globalPool.mu.RUnlock()
		return v
	}
	globalPool.mu.RUnlock()

	globalPool.mu.Lock()
	defer globalPool.mu.Unlock()

	// OOM Protection: Bounded capacity check
	if len(globalPool.entries) >= DefaultMaxInternPoolSize {
		return s
	}

	if v, ok := globalPool.entries[s]; ok {
		return v
	}

	cloned := string([]byte(s))
	globalPool.entries[cloned] = cloned
	return cloned
}

// Bytes interns a string derived from a byte slice without heap allocation when present in the pool.
func Bytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if len(b) > DefaultMaxInternStringLen {
		return string(b)
	}
	view := unsafe.String(unsafe.SliceData(b), len(b))
	return String(view)
}
