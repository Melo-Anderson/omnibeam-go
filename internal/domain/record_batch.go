package domain

import (
	"fmt"
	"sync"
)

// IsNumericDataType reports whether t is stored in RecordBatch.NumCols.
// Types stored in NumCols share uint64 storage (mirroring FieldValue.NumVal):
// TypeInt64, TypeFloat64, TypeBool, TypeTimestamp, TypeDecimal.
func IsNumericDataType(t DataType) bool {
	switch t {
	case TypeInt64, TypeFloat64, TypeBool, TypeTimestamp, TypeDecimal:
		return true
	}
	return false
}

// RecordBatch represents a Structure-of-Arrays (SoA) columnar block of rows.
// It stores data in contiguous columnar slices to minimize pointer dereferencing
// and achieve high cache locality during batch processing.
type RecordBatch struct {
	schema   Schema
	capacity int
	rowCount int

	// Columnar contiguous storage.
	// NumCols: stores uint64 scalar bits (TypeInt64, TypeFloat64, TypeBool, TypeTimestamp, TypeDecimal).
	// StrCols: stores string-like values (TypeString, TypeBytes, TypeDate, TypeJSON).
	// ScaleCols: stores per-row scale for TypeDecimal columns.
	NumCols   [][]uint64
	StrCols   [][]string
	ScaleCols [][]int32

	// NullBitmaps: 1 bit per row per field (1 = null, 0 = present). 64 rows per uint64 word.
	NullBitmaps [][]uint64

	// ValidMask: 1 bit per row indicating if the row is valid (1 = valid, 0 = invalid/quarantined).
	ValidMask []uint64

	// AuditValues: per-row audit metadata references.
	AuditValues []map[string]string
}

// RecordBatchPool provides a sync.Pool for RecordBatch objects to reuse memory
// and eliminate heap allocations during continuous high-throughput pipeline execution.
type RecordBatchPool struct {
	pool sync.Pool
}

// NewRecordBatchPool creates a new pool for RecordBatch of a given schema and capacity.
func NewRecordBatchPool(schema Schema, capacity int) *RecordBatchPool {
	return &RecordBatchPool{
		pool: sync.Pool{
			New: func() any {
				return NewRecordBatch(schema, capacity)
			},
		},
	}
}

// Get retrieves a RecordBatch from the pool and resets it for reuse.
func (p *RecordBatchPool) Get() *RecordBatch {
	batch := p.pool.Get().(*RecordBatch)
	batch.Reset()
	return batch
}

// Put returns a RecordBatch to the pool for subsequent reuse.
func (p *RecordBatchPool) Put(batch *RecordBatch) {
	if batch != nil {
		p.pool.Put(batch)
	}
}

// NewRecordBatch creates an initialized RecordBatch with pre-allocated column vectors.
func NewRecordBatch(schema Schema, capacity int) *RecordBatch {
	capacity = OrDefault(capacity, DefaultRecordBatchCapacity)
	if capacity < DefaultMinRecordBatchCapacity {
		capacity = DefaultMinRecordBatchCapacity
	}
	numWords := (capacity + 63) / 64

	nFields := len(schema.Fields)
	b := &RecordBatch{
		schema:      schema,
		capacity:    capacity,
		rowCount:    0,
		NumCols:     make([][]uint64, nFields),
		StrCols:     make([][]string, nFields),
		ScaleCols:   make([][]int32, nFields),
		NullBitmaps: make([][]uint64, nFields),
		ValidMask:   make([]uint64, numWords),
		AuditValues: make([]map[string]string, 0, capacity),
	}

	for i, f := range schema.Fields {
		b.NullBitmaps[i] = make([]uint64, numWords)
		if IsNumericDataType(f.Type) {
			b.NumCols[i] = make([]uint64, 0, capacity)
			if f.Type == TypeDecimal {
				b.ScaleCols[i] = make([]int32, 0, capacity)
			}
		} else {
			b.StrCols[i] = make([]string, 0, capacity)
		}
	}

	// Initially mark all capacity slots as valid
	for i := range b.ValidMask {
		b.ValidMask[i] = ^uint64(0)
	}

	return b
}

func (b *RecordBatch) Schema() Schema   { return b.schema }
func (b *RecordBatch) Capacity() int   { return b.capacity }
func (b *RecordBatch) RowCount() int   { return b.rowCount }
func (b *RecordBatch) SetRowCount(n int) { b.rowCount = n }

// AppendRow appends a GenericRecord into the columnar batch.
func (b *RecordBatch) AppendRow(rec *GenericRecord) error {
	if b.rowCount >= b.capacity {
		return fmt.Errorf("RecordBatch: capacity %d exceeded", b.capacity)
	}
	idx := b.rowCount
	wordIdx := idx / 64
	bitIdx := uint(idx % 64)

	for i, f := range b.schema.Fields {
		if rec == nil || i >= len(rec.Values) || rec.Values[i].IsNull {
			b.NullBitmaps[i][wordIdx] |= (1 << bitIdx)
			if IsNumericDataType(f.Type) {
				b.NumCols[i] = append(b.NumCols[i], 0)
				if f.Type == TypeDecimal {
					b.ScaleCols[i] = append(b.ScaleCols[i], 0)
				}
			} else {
				b.StrCols[i] = append(b.StrCols[i], "")
			}
			continue
		}

		val := rec.Values[i]
		if IsNumericDataType(f.Type) {
			b.NumCols[i] = append(b.NumCols[i], val.NumVal)
			if f.Type == TypeDecimal {
				b.ScaleCols[i] = append(b.ScaleCols[i], val.Scale)
			}
		} else {
			b.StrCols[i] = append(b.StrCols[i], val.StrVal)
		}
	}

	if rec != nil {
		b.AuditValues = append(b.AuditValues, rec.AuditFields)
	} else {
		b.AuditValues = append(b.AuditValues, nil)
	}

	b.rowCount++
	return nil
}

// SetRowValid sets the validity bit for a given row index.
func (b *RecordBatch) SetRowValid(idx int, valid bool) {
	if idx < 0 || idx >= b.rowCount {
		return
	}
	word := idx / 64
	bit := uint(idx % 64)
	if valid {
		b.ValidMask[word] |= (1 << bit)
	} else {
		b.ValidMask[word] &^= (1 << bit)
	}
}

// IsRowValid reports whether the row at index idx is marked valid.
func (b *RecordBatch) IsRowValid(idx int) bool {
	if idx < 0 || idx >= b.rowCount {
		return false
	}
	word := idx / 64
	bit := uint(idx % 64)
	return (b.ValidMask[word] & (1 << bit)) != 0
}

// ValidRowCount returns the number of valid (non-quarantined) rows in the batch.
func (b *RecordBatch) ValidRowCount() int {
	valid := 0
	for i := 0; i < b.rowCount; i++ {
		if b.IsRowValid(i) {
			valid++
		}
	}
	return valid
}

// Reset clears the batch state for reuse while retaining allocated slice capacities.
func (b *RecordBatch) Reset() {
	b.rowCount = 0
	numWords := (b.capacity + 63) / 64
	for i := 0; i < numWords; i++ {
		b.ValidMask[i] = ^uint64(0)
	}
	for i, f := range b.schema.Fields {
		for w := 0; w < numWords; w++ {
			b.NullBitmaps[i][w] = 0
		}
		if IsNumericDataType(f.Type) {
			b.NumCols[i] = b.NumCols[i][:0]
			if f.Type == TypeDecimal {
				b.ScaleCols[i] = b.ScaleCols[i][:0]
			}
		} else {
			b.StrCols[i] = b.StrCols[i][:0]
		}
	}
	for i := range b.AuditValues {
		b.AuditValues[i] = nil
	}
	b.AuditValues = b.AuditValues[:0]
}

// RecordBatchIterator provides a zero-copy row view over RecordBatch.
// It skips rows marked invalid (quarantined) and yields GenericRecord references.
type RecordBatchIterator struct {
	batch  *RecordBatch
	curr   int
	cached GenericRecord
}

// Iterator returns a new zero-copy iterator over valid rows in the batch.
func (b *RecordBatch) Iterator() *RecordBatchIterator {
	return &RecordBatchIterator{
		batch: b,
		curr:  -1,
		cached: GenericRecord{
			Values:      make([]FieldValue, len(b.schema.Fields)),
			AuditFields: make(map[string]string),
		},
	}
}

// Next advances the iterator to the next valid row and reports whether a row is available.
func (it *RecordBatchIterator) Next() bool {
	it.curr++
	for it.curr < it.batch.rowCount && !it.batch.IsRowValid(it.curr) {
		it.curr++
	}
	return it.curr < it.batch.rowCount
}

// Record returns a pointer to the cached GenericRecord populated with the current row's data.
func (it *RecordBatchIterator) Record() *GenericRecord {
	idx := it.curr
	word := idx / 64
	bit := uint(idx % 64)

	for i, f := range it.batch.schema.Fields {
		isNull := (it.batch.NullBitmaps[i][word] & (1 << bit)) != 0
		it.cached.Values[i].IsNull = isNull
		it.cached.Values[i].Type = f.Type
		if isNull {
			it.cached.Values[i].NumVal = 0
			it.cached.Values[i].StrVal = ""
			it.cached.Values[i].Scale = 0
			continue
		}

		if IsNumericDataType(f.Type) {
			it.cached.Values[i].NumVal = it.batch.NumCols[i][idx]
			if f.Type == TypeDecimal {
				it.cached.Values[i].Scale = it.batch.ScaleCols[i][idx]
			} else {
				it.cached.Values[i].Scale = 0
			}
			it.cached.Values[i].StrVal = ""
		} else {
			it.cached.Values[i].StrVal = it.batch.StrCols[i][idx]
			it.cached.Values[i].NumVal = 0
			it.cached.Values[i].Scale = 0
		}
	}

	if idx < len(it.batch.AuditValues) {
		it.cached.AuditFields = it.batch.AuditValues[idx]
	} else {
		it.cached.AuditFields = nil
	}

	return &it.cached
}
