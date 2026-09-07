// Package fastnum provides zero-allocation, high-performance numerical parsing.
package fastnum

import (
	"errors"
	"math"
	"strings"
)

var (
	// ErrInvalidInt is returned when an integer string format is malformed.
	ErrInvalidInt = errors.New("fastnum: invalid integer string")
	// ErrInvalidFloat is returned when a float string format is malformed.
	ErrInvalidFloat = errors.New("fastnum: invalid float string")
	// ErrOverflow is returned when a parsed integer exceeds int64 bounds.
	ErrOverflow = errors.New("fastnum: integer overflow")
)

// ParseInt64 parses decimal integer strings rapidly without locale overhead.
// Accepts optional leading/trailing whitespace and an optional sign prefix.
func ParseInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, ErrInvalidInt
	}
	neg, i := extractSign(s)
	if i == len(s) {
		return 0, ErrInvalidInt
	}
	n, err := parseDigitsUint64(s[i:])
	if err != nil {
		return 0, err
	}
	if neg {
		if n > uint64(math.MaxInt64)+1 {
			return 0, ErrOverflow
		}
		return -int64(n), nil
	}
	if n > math.MaxInt64 {
		return 0, ErrOverflow
	}
	return int64(n), nil
}

// ParseFloat64 parses decimal float strings at high speed.
// Supports integer-only input (e.g. "100") and leading dot (e.g. ".5").
func ParseFloat64(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, ErrInvalidFloat
	}
	neg, i := extractSign(s)
	if i == len(s) {
		return 0, ErrInvalidFloat
	}
	val, err := parseFractional(s[i:])
	if err != nil {
		return 0, err
	}
	if neg {
		return -val, nil
	}
	return val, nil
}

func extractSign(s string) (bool, int) {
	if s[0] == '-' {
		return true, 1
	}
	if s[0] == '+' {
		return false, 1
	}
	return false, 0
}

func parseDigitsUint64(s string) (uint64, error) {
	var n uint64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, ErrInvalidInt
		}
		digit := uint64(c - '0')
		if n > (math.MaxUint64-digit)/10 {
			return 0, ErrOverflow
		}
		n = n*10 + digit
	}
	return n, nil
}

func parseFractional(s string) (float64, error) {
	var intPart uint64
	hasDigits := false
	i := 0
	for ; i < len(s) && s[i] != '.'; i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, ErrInvalidFloat
		}
		intPart = intPart*10 + uint64(c-'0')
		hasDigits = true
	}
	val := float64(intPart)
	if i < len(s) && s[i] == '.' {
		i++
		factor := 0.1
		for ; i < len(s); i++ {
			c := s[i]
			if c < '0' || c > '9' {
				return 0, ErrInvalidFloat
			}
			val += float64(c-'0') * factor
			factor *= 0.1
			hasDigits = true
		}
	}
	if !hasDigits {
		return 0, ErrInvalidFloat
	}
	return val, nil
}
