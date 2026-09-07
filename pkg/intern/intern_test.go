package intern_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/omnibeam/dataflow-compute-go/pkg/intern"
)

func TestIntern_String_SharesPointer(t *testing.T) {
	intern.ResetForTesting()

	s1 := string([]byte("ACTIVE"))
	s2 := string([]byte("ACTIVE"))

	i1 := intern.String(s1)
	i2 := intern.String(s2)

	if i1 != "ACTIVE" || i2 != "ACTIVE" {
		t.Fatalf("interned value mismatch: %s, %s", i1, i2)
	}
	if unsafe.StringData(i1) != unsafe.StringData(i2) {
		t.Errorf("interned strings must share the same underlying data pointer")
	}
}

func TestIntern_Bytes_SharesPointer(t *testing.T) {
	intern.ResetForTesting()

	b1 := []byte("COMPLETED")
	b2 := []byte("COMPLETED")

	i1 := intern.Bytes(b1)
	i2 := intern.Bytes(b2)

	if i1 != "COMPLETED" || i2 != "COMPLETED" {
		t.Fatalf("interned value mismatch: %s, %s", i1, i2)
	}
	if unsafe.StringData(i1) != unsafe.StringData(i2) {
		t.Errorf("Bytes interning must share pointer for equal content")
	}
}

func TestIntern_EmptyString(t *testing.T) {
	intern.ResetForTesting()

	if intern.String("") != "" {
		t.Error("empty string must return empty string")
	}
	if intern.Bytes(nil) != "" {
		t.Error("nil bytes must return empty string")
	}
	if intern.Bytes([]byte{}) != "" {
		t.Error("empty byte slice must return empty string")
	}
}

func TestString_BasicAndLengthLimit(t *testing.T) {
	intern.ResetForTesting()

	// Short string should be interned
	s1 := "status_active"
	s2 := string([]byte("status_active"))
	if intern.String(s1) != intern.String(s2) {
		t.Fatalf("expected identical string content")
	}

	// Long string (> DefaultMaxInternStringLen) should NOT be interned (returns original directly)
	longStr := strings.Repeat("a", intern.DefaultMaxInternStringLen+10)
	got := intern.String(longStr)
	if got != longStr {
		t.Fatalf("expected original long string")
	}
}

func TestBytes_DerivedString_LengthLimit(t *testing.T) {
	intern.ResetForTesting()

	b := []byte("category_A")
	s := intern.Bytes(b)
	if s != "category_A" {
		t.Fatalf("expected category_A, got %s", s)
	}

	// Long byte slice (> DefaultMaxInternStringLen)
	longBytes := []byte(strings.Repeat("z", intern.DefaultMaxInternStringLen+10))
	gotLong := intern.Bytes(longBytes)
	if gotLong != string(longBytes) {
		t.Fatalf("expected exact string copy for long bytes")
	}
}

func TestString_PoolCapacityLimit(t *testing.T) {
	intern.ResetForTesting()

	// Fill pool beyond capacity
	for i := 0; i < intern.DefaultMaxInternPoolSize+1000; i++ {
		s := fmt.Sprintf("val_%d", i)
		_ = intern.String(s)
	}

	// Any subsequent string should still return valid string without crashing or leaking unbounded memory
	res := intern.String("overflow_string")
	if res != "overflow_string" {
		t.Fatalf("expected valid string on pool overflow, got %s", res)
	}
}

func TestIntern_Concurrency(t *testing.T) {
	intern.ResetForTesting()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s := intern.String(fmt.Sprintf("key_%d", j%10))
				if s != fmt.Sprintf("key_%d", j%10) {
					t.Errorf("unexpected string value: %s", s)
				}
				b := intern.Bytes([]byte(fmt.Sprintf("key_%d", j%10)))
				if b != fmt.Sprintf("key_%d", j%10) {
					t.Errorf("unexpected bytes value: %s", b)
				}
			}
		}(i)
	}
	wg.Wait()
}
