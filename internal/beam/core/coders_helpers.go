package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

var ErrNilToken = errors.New("coders: nil token")

var maskPool = sync.Pool{
	New: func() any {
		b := make([]byte, domain.DefaultMaskPoolBytes)
		return &b
	},
}

func writeNullMask(w io.Writer, values []domain.FieldValue) error {
	maskLen := (len(values) + 7) / 8
	var stackMask [domain.DefaultStackMaskBytes]byte
	var mask []byte
	if maskLen <= len(stackMask) {
		mask = stackMask[:maskLen]
		for i := range mask {
			mask[i] = 0
		}
	} else {
		bufPtr := maskPool.Get().(*[]byte)
		if len(*bufPtr) < maskLen {
			*bufPtr = make([]byte, maskLen)
		}
		mask = (*bufPtr)[:maskLen]
		for i := range mask {
			mask[i] = 0
		}
		defer maskPool.Put(bufPtr)
	}

	for i, fv := range values {
		if fv.IsNull {
			mask[i/8] |= 1 << (i % 8)
		}
	}
	_, err := w.Write(mask)
	return err
}

func readNullMask(r io.Reader, n int) ([]byte, error) {
	return readNullMaskWithBuf(r, n, nil)
}

func readNullMaskWithBuf(r io.Reader, n int, stackBuf []byte) ([]byte, error) {
	maskLen := (n + 7) / 8
	var mask []byte
	if len(stackBuf) >= maskLen {
		mask = stackBuf[:maskLen]
	} else {
		mask = make([]byte, maskLen)
	}
	if _, err := io.ReadFull(r, mask); err != nil {
		return nil, fmt.Errorf("coders: nullMask: %w", err)
	}
	return mask, nil
}

func writeFieldValues(w io.Writer, values []domain.FieldValue) error {
	var typeBuf [1]byte
	for _, fv := range values {
		if fv.IsNull {
			continue
		}
		typeBuf[0] = dataTypeToByte(fv.Type)
		if _, err := w.Write(typeBuf[:]); err != nil {
			return err
		}
		if err := encodeFieldValue(w, fv); err != nil {
			return err
		}
	}
	return nil
}

func readFieldValues(r io.Reader, values []domain.FieldValue, mask []byte) error {
	var typeBuf [1]byte
	for i := range values {
		if mask[i/8]&(1<<(i%8)) != 0 {
			values[i].IsNull = true
			continue
		}
		if _, err := io.ReadFull(r, typeBuf[:]); err != nil {
			return fmt.Errorf("coders: fieldType[%d]: %w", i, err)
		}
		dt := byteToDataType(typeBuf[0])
		if err := decodeFieldValue(r, &values[i], dt); err != nil {
			return fmt.Errorf("coders: field[%d]: %w", i, err)
		}
	}
	return nil
}

func dataTypeToByte(dt domain.DataType) byte {
	switch dt {
	case domain.TypeInt64:
		return 1
	case domain.TypeFloat64:
		return 2
	case domain.TypeBool:
		return 3
	case domain.TypeTimestamp:
		return 4
	case domain.TypeBytes:
		return 5
	case domain.TypeDate:
		return 6
	case domain.TypeDecimal:
		return 7
	case domain.TypeJSON:
		return 8
	default:
		return 0
	}
}

func byteToDataType(b byte) domain.DataType {
	switch b {
	case 1:
		return domain.TypeInt64
	case 2:
		return domain.TypeFloat64
	case 3:
		return domain.TypeBool
	case 4:
		return domain.TypeTimestamp
	case 5:
		return domain.TypeBytes
	case 6:
		return domain.TypeDate
	case 7:
		return domain.TypeDecimal
	case 8:
		return domain.TypeJSON
	default:
		return domain.TypeString
	}
}

func writeAuditFields(w io.Writer, fields map[string]string) error {
	if err := writeVarint(w, int64(len(fields))); err != nil {
		return err
	}
	for k, v := range fields {
		if err := writeString(w, k); err != nil {
			return err
		}
		if err := writeString(w, v); err != nil {
			return err
		}
	}
	return nil
}

func readAuditFields(r io.Reader, fields map[string]string) error {
	n64, err := readVarint(r)
	if err != nil {
		return fmt.Errorf("coders: auditCount: %w", err)
	}
	for i := int64(0); i < n64; i++ {
		k, err := readString(r)
		if err != nil {
			return err
		}
		v, err := readString(r)
		if err != nil {
			return err
		}
		fields[k] = v
	}
	return nil
}

func encodeFieldValue(w io.Writer, fv domain.FieldValue) error {
	switch fv.Type {
	case domain.TypeInt64:
		return writeVarint(w, fv.Int64Val())
	case domain.TypeFloat64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], fv.NumVal)
		_, err := w.Write(buf[:])
		return err
	case domain.TypeBool:
		var b [1]byte
		if fv.BoolVal() {
			b[0] = 1
		}
		_, err := w.Write(b[:])
		return err
	case domain.TypeTimestamp:
		return writeVarint(w, int64(fv.NumVal))
	case domain.TypeBytes:
		return writeBytes(w, fv.BytesVal())
	case domain.TypeDecimal:
		if err := writeVarint(w, fv.Int64Val()); err != nil {
			return err
		}
		return writeVarint(w, int64(fv.Scale))
	default:
		return writeString(w, fv.StrVal)
	}
}

func decodeFieldValue(r io.Reader, fv *domain.FieldValue, dt domain.DataType) error {
	fv.Type = dt
	switch dt {
	case domain.TypeInt64:
		v, err := readVarint(r)
		fv.NumVal = uint64(v)
		return err
	case domain.TypeFloat64:
		var buf [8]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return err
		}
		fv.NumVal = binary.BigEndian.Uint64(buf[:])
		return nil
	case domain.TypeBool:
		var buf [1]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return err
		}
		num := uint64(0)
		if buf[0] != 0 {
			num = 1
		}
		fv.NumVal = num
		return nil
	case domain.TypeTimestamp:
		ns, err := readVarint(r)
		fv.NumVal = uint64(ns)
		return err
	case domain.TypeBytes:
		b, err := readBytes(r)
		fv.StrVal = string(b)
		return err
	case domain.TypeDecimal:
		m, err := readVarint(r)
		if err != nil {
			return err
		}
		s, err := readVarint(r)
		if err != nil {
			return err
		}
		fv.NumVal = uint64(m)
		fv.Scale = int32(s)
		return nil
	default:
		s, err := readString(r)
		fv.StrVal = s
		return err
	}
}

func writeVarint(w io.Writer, v int64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], v)
	_, err := w.Write(buf[:n])
	return err
}

type byteReader struct {
	r   io.Reader
	buf [1]byte
}

func (b *byteReader) ReadByte() (byte, error) {
	_, err := io.ReadFull(b.r, b.buf[:])
	return b.buf[0], err
}

func readVarint(r io.Reader) (int64, error) {
	if br, ok := r.(io.ByteReader); ok {
		return binary.ReadVarint(br)
	}
	return binary.ReadVarint(&byteReader{r: r})
}

func writeString(w io.Writer, s string) error {
	return writeBytes(w, []byte(s))
}

func readString(r io.Reader) (string, error) {
	b, err := readBytes(r)
	return string(b), err
}

func writeBytes(w io.Writer, b []byte) error {
	if err := writeVarint(w, int64(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readBytes(r io.Reader) ([]byte, error) {
	n, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, ErrNilToken
	}
	if n < -1 || n > domain.DefaultChunkSizeBytes {
		return nil, fmt.Errorf("coders: invalid byte length %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}
