package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/omnibeam/dataflow-compute-go/pkg/fastnum"
)

var boolStringMap = map[string]bool{
	"true": true, "t": true, "1": true, "yes": true, "y": true,
	"false": false, "f": false, "0": false, "no": false, "n": false,
}

// CoerceInt64Fast parses a decimal integer string into int64.
func CoerceInt64Fast(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("CoerceInt64Fast: empty string")
	}
	return fastnum.ParseInt64(s)
}

// CoerceFloat64Fast parses a floating-point string into float64.
func CoerceFloat64Fast(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("CoerceFloat64Fast: empty string")
	}
	return fastnum.ParseFloat64(s)
}

// CoerceBoolFast parses a boolean string into bool.
func CoerceBoolFast(s string) (bool, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if b, ok := boolStringMap[s]; ok {
		return b, nil
	}
	return false, fmt.Errorf("CoerceBoolFast: cannot parse %q as bool", s)
}

// CoerceTimestampFast parses a timestamp string into time.Time UTC.
func CoerceTimestampFast(s string) (time.Time, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("CoerceTimestampFast: empty string")
	}
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, trimmed); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("CoerceTimestampFast: cannot parse %q", s)
}

// CoerceDecimalFast parses a decimal string into fixed-point mantissa and scale
// using a zero-allocation single-pass mathematical loop.
// onOverflow supports "round" (default half-up) or "fail" (returns error on excess fractional digits).
func CoerceDecimalFast(s string, targetScale int32, onOverflow string) (int64, int32, error) {
	if onOverflow == "" {
		onOverflow = "round"
	}
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, targetScale, nil
	}

	neg := false
	i := 0
	if s[0] == '-' {
		neg = true
		i++
	} else if s[0] == '+' {
		i++
	}

	if i >= len(s) {
		return 0, 0, fmt.Errorf("invalid decimal %q: empty digits", s)
	}

	var mantissa uint64
	seenDot := false
	var decDigits int32 = 0
	roundUp := false

	for ; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			if seenDot {
				return 0, 0, fmt.Errorf("invalid decimal %q: multiple decimal points", s)
			}
			seenDot = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, 0, fmt.Errorf("invalid decimal %q: unexpected character %q", s, c)
		}

		digit := uint64(c - '0')

		if !seenDot {
			mantissa = mantissa*10 + digit
		} else {
			if decDigits < targetScale {
				mantissa = mantissa*10 + digit
				decDigits++
			} else if decDigits == targetScale {
				if onOverflow == "fail" {
					return 0, 0, fmt.Errorf("%w: fractional precision exceeds scale %d in %q", ErrTypeCastFailed, targetScale, s)
				}
				if digit >= 5 {
					roundUp = true
				}
				decDigits++
			}
		}
	}

	for decDigits < targetScale {
		mantissa *= 10
		decDigits++
	}

	if roundUp {
		mantissa++
	}

	res := int64(mantissa)
	if neg {
		res = -res
	}
	return res, targetScale, nil
}
