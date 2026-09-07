package core

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// RecordBatchEncoder serializes a RecordBatch into a compact binary format.
// Format:
//
//	[Header: rowCount uint32, numFields uint32]
//	[ValidMask: numWords uint64...]
//	For each column:
//	  [NullBitmap: numWords uint64...]
//	  If Numeric:
//	    [NumCols: rowCount uint64...]
//	    If Decimal:
//	      [ScaleCols: rowCount int32...]
//	  Else:
//	    [StrCols: (len uint32, bytes)...]
//	[AuditValues: per row (count uint16, (keyLen, key, valLen, val)...)]
type RecordBatchEncoder struct {
	w io.Writer
}

// NewRecordBatchEncoder constructs a RecordBatchEncoder writing to w.
func NewRecordBatchEncoder(w io.Writer) *RecordBatchEncoder {
	return &RecordBatchEncoder{w: w}
}

// Encode writes b to the underlying io.Writer.
func (enc *RecordBatchEncoder) Encode(b *domain.RecordBatch) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(b.RowCount()))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(b.Schema().Fields)))
	if _, err := enc.w.Write(hdr[:]); err != nil {
		return err
	}

	rowCount := b.RowCount()
	numWords := (rowCount + 63) / 64

	// 1. Write validity mask
	for i := 0; i < numWords; i++ {
		var maskBuf [8]byte
		binary.LittleEndian.PutUint64(maskBuf[:], b.ValidMask[i])
		if _, err := enc.w.Write(maskBuf[:]); err != nil {
			return err
		}
	}

	// 2. Write null bitmaps and column vectors
	for colIdx, f := range b.Schema().Fields {
		for i := 0; i < numWords; i++ {
			var nullBuf [8]byte
			binary.LittleEndian.PutUint64(nullBuf[:], b.NullBitmaps[colIdx][i])
			if _, err := enc.w.Write(nullBuf[:]); err != nil {
				return err
			}
		}

		if domain.IsNumericDataType(f.Type) {
			for _, v := range b.NumCols[colIdx] {
				var vBuf [8]byte
				binary.LittleEndian.PutUint64(vBuf[:], v)
				if _, err := enc.w.Write(vBuf[:]); err != nil {
					return err
				}
			}
			if f.Type == domain.TypeDecimal {
				for _, s := range b.ScaleCols[colIdx] {
					var sBuf [4]byte
					binary.LittleEndian.PutUint32(sBuf[:], uint32(s))
					if _, err := enc.w.Write(sBuf[:]); err != nil {
						return err
					}
				}
			}
		} else {
			for _, str := range b.StrCols[colIdx] {
				strBytes := []byte(str)
				var lenBuf [4]byte
				binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(strBytes)))
				if _, err := enc.w.Write(lenBuf[:]); err != nil {
					return err
				}
				if len(strBytes) > 0 {
					if _, err := enc.w.Write(strBytes); err != nil {
						return err
					}
				}
			}
		}
	}

	// 3. Write Audit Values
	for row := 0; row < rowCount; row++ {
		var m map[string]string
		if row < len(b.AuditValues) {
			m = b.AuditValues[row]
		}
		var countBuf [2]byte
		binary.LittleEndian.PutUint16(countBuf[:], uint16(len(m)))
		if _, err := enc.w.Write(countBuf[:]); err != nil {
			return err
		}
		for k, v := range m {
			kBytes := []byte(k)
			vBytes := []byte(v)
			var lenBuf [4]byte
			binary.LittleEndian.PutUint16(lenBuf[0:2], uint16(len(kBytes)))
			binary.LittleEndian.PutUint16(lenBuf[2:4], uint16(len(vBytes)))
			if _, err := enc.w.Write(lenBuf[:]); err != nil {
				return err
			}
			if _, err := enc.w.Write(kBytes); err != nil {
				return err
			}
			if _, err := enc.w.Write(vBytes); err != nil {
				return err
			}
		}
	}

	return nil
}

// RecordBatchDecoder deserializes a RecordBatch from an io.Reader given a Schema.
type RecordBatchDecoder struct {
	r io.Reader
}

// NewRecordBatchDecoder constructs a RecordBatchDecoder reading from r.
func NewRecordBatchDecoder(r io.Reader) *RecordBatchDecoder {
	return &RecordBatchDecoder{r: r}
}

// Decode reads and parses a RecordBatch from the underlying reader.
func (dec *RecordBatchDecoder) Decode(schema domain.Schema) (*domain.RecordBatch, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(dec.r, hdr[:]); err != nil {
		return nil, err
	}
	rowCount := int(binary.LittleEndian.Uint32(hdr[0:4]))
	numFields := int(binary.LittleEndian.Uint32(hdr[4:8]))

	if numFields != len(schema.Fields) {
		return nil, fmt.Errorf("RecordBatchDecoder: schema field count mismatch (expected %d, got %d)", len(schema.Fields), numFields)
	}

	batch := domain.NewRecordBatch(schema, rowCount)
	numWords := (rowCount + 63) / 64

	// 1. Read validity mask
	for i := 0; i < numWords; i++ {
		var maskBuf [8]byte
		if _, err := io.ReadFull(dec.r, maskBuf[:]); err != nil {
			return nil, err
		}
		batch.ValidMask[i] = binary.LittleEndian.Uint64(maskBuf[:])
	}

	// 2. Read null bitmaps and column vectors
	for colIdx, f := range schema.Fields {
		for i := 0; i < numWords; i++ {
			var nullBuf [8]byte
			if _, err := io.ReadFull(dec.r, nullBuf[:]); err != nil {
				return nil, err
			}
			batch.NullBitmaps[colIdx][i] = binary.LittleEndian.Uint64(nullBuf[:])
		}

		if domain.IsNumericDataType(f.Type) {
			for row := 0; row < rowCount; row++ {
				var vBuf [8]byte
				if _, err := io.ReadFull(dec.r, vBuf[:]); err != nil {
					return nil, err
				}
				batch.NumCols[colIdx] = append(batch.NumCols[colIdx], binary.LittleEndian.Uint64(vBuf[:]))
			}
			if f.Type == domain.TypeDecimal {
				for row := 0; row < rowCount; row++ {
					var sBuf [4]byte
					if _, err := io.ReadFull(dec.r, sBuf[:]); err != nil {
						return nil, err
					}
					batch.ScaleCols[colIdx] = append(batch.ScaleCols[colIdx], int32(binary.LittleEndian.Uint32(sBuf[:])))
				}
			}
		} else {
			for row := 0; row < rowCount; row++ {
				var lenBuf [4]byte
				if _, err := io.ReadFull(dec.r, lenBuf[:]); err != nil {
					return nil, err
				}
				sLen := binary.LittleEndian.Uint32(lenBuf[:])
				if sLen == 0 {
					batch.StrCols[colIdx] = append(batch.StrCols[colIdx], "")
					continue
				}
				strBytes := make([]byte, sLen)
				if _, err := io.ReadFull(dec.r, strBytes); err != nil {
					return nil, err
				}
				batch.StrCols[colIdx] = append(batch.StrCols[colIdx], string(strBytes))
			}
		}
	}

	// 3. Read Audit Values
	for row := 0; row < rowCount; row++ {
		var countBuf [2]byte
		if _, err := io.ReadFull(dec.r, countBuf[:]); err != nil {
			return nil, err
		}
		count := int(binary.LittleEndian.Uint16(countBuf[:]))
		if count == 0 {
			batch.AuditValues = append(batch.AuditValues, nil)
			continue
		}
		m := make(map[string]string, count)
		for i := 0; i < count; i++ {
			var lenBuf [4]byte
			if _, err := io.ReadFull(dec.r, lenBuf[:]); err != nil {
				return nil, err
			}
			kLen := int(binary.LittleEndian.Uint16(lenBuf[0:2]))
			vLen := int(binary.LittleEndian.Uint16(lenBuf[2:4]))
			kBytes := make([]byte, kLen)
			if _, err := io.ReadFull(dec.r, kBytes); err != nil {
				return nil, err
			}
			vBytes := make([]byte, vLen)
			if _, err := io.ReadFull(dec.r, vBytes); err != nil {
				return nil, err
			}
			m[string(kBytes)] = string(vBytes)
		}
		batch.AuditValues = append(batch.AuditValues, m)
	}

	batch.SetRowCount(rowCount)
	return batch, nil
}
