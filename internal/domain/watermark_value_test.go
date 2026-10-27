package domain_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestWatermarkValue_CompareIsNumericNotLexicographic(t *testing.T) {
	nine, err := domain.ParseWatermarkValue(domain.WatermarkTypeInteger, "9")
	if err != nil {
		t.Fatalf("unexpected error parsing 9: %v", err)
	}
	ten, err := domain.ParseWatermarkValue(domain.WatermarkTypeInteger, "10")
	if err != nil {
		t.Fatalf("unexpected error parsing 10: %v", err)
	}
	if ten.Compare(nine) != 1 {
		t.Fatalf("10 must be > 9, got compare result %d", ten.Compare(nine))
	}
	if nine.Compare(ten) != -1 {
		t.Fatalf("9 must be < 10, got compare result %d", nine.Compare(ten))
	}
	nineAgain, _ := domain.ParseWatermarkValue(domain.WatermarkTypeInteger, "9")
	if nine.Compare(nineAgain) != 0 {
		t.Fatalf("9 must be equal to 9, got compare result %d", nine.Compare(nineAgain))
	}

	// Floats
	f9, _ := domain.ParseWatermarkValue(domain.WatermarkTypeFloat, "9.8")
	f10, _ := domain.ParseWatermarkValue(domain.WatermarkTypeFloat, "10.5")
	if f10.Compare(f9) != 1 {
		t.Fatalf("10.5 must be > 9.8")
	}
}

func TestWatermarkValue_CompareTimestampsByInstant(t *testing.T) {
	// "2026-10-24T10:00:00+02:00" is 08:00 UTC, which is earlier than "2026-10-24T09:00:00+00:00" (09:00 UTC)
	t1, err := domain.ParseWatermarkValue(domain.WatermarkTypeTimestamp, "2026-10-24T10:00:00+02:00")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	t2, err := domain.ParseWatermarkValue(domain.WatermarkTypeTimestamp, "2026-10-24T09:00:00+00:00")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if t1.Compare(t2) != -1 {
		t.Fatalf("t1 (08:00 UTC) should be < t2 (09:00 UTC), got %d", t1.Compare(t2))
	}
}

func TestWatermarkValue_StringIsCanonical(t *testing.T) {
	// "2026-10-24T10:14:22Z" -> "2026-10-24T10:14:22+00:00"
	wmTs, err := domain.ParseWatermarkValue(domain.WatermarkTypeTimestamp, "2026-10-24T10:14:22Z")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if wmTs.String() != "2026-10-24T10:14:22+00:00" {
		t.Fatalf("expected canonical +00:00 timestamp, got %s", wmTs.String())
	}

	wmDate, err := domain.ParseWatermarkValue(domain.WatermarkTypeDate, "2026-10-24")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if wmDate.String() != "2026-10-24" {
		t.Fatalf("expected 2026-10-24, got %s", wmDate.String())
	}

	wmInt, err := domain.ParseWatermarkValue(domain.WatermarkTypeInteger, "1500")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if wmInt.String() != "1500" {
		t.Fatalf("expected 1500, got %s", wmInt.String())
	}
}

func TestWatermarkValue_SQLLiteral(t *testing.T) {
	intVal, _ := domain.ParseWatermarkValue(domain.WatermarkTypeInteger, "1500")
	if lit := intVal.SQLLiteral(); lit != "1500" {
		t.Fatalf("expected 1500, got %s", lit)
	}

	floatVal, _ := domain.ParseWatermarkValue(domain.WatermarkTypeFloat, "1500.5")
	if lit := floatVal.SQLLiteral(); lit != "1500.5" {
		t.Fatalf("expected 1500.5, got %s", lit)
	}

	strVal, _ := domain.ParseWatermarkValue(domain.WatermarkTypeString, "O'Brien")
	if lit := strVal.SQLLiteral(); lit != "'O''Brien'" {
		t.Fatalf("expected 'O''Brien', got %s", lit)
	}

	tsVal, _ := domain.ParseWatermarkValue(domain.WatermarkTypeTimestamp, "2026-10-24T10:14:22Z")
	if lit := tsVal.SQLLiteral(); lit != "'2026-10-24T10:14:22+00:00'" {
		t.Fatalf("expected '2026-10-24T10:14:22+00:00', got %s", lit)
	}
}

func TestParseWatermarkValue_RejectsInvalid(t *testing.T) {
	cases := []struct {
		t   domain.WatermarkType
		val string
	}{
		{domain.WatermarkTypeInteger, "1.5"},
		{domain.WatermarkTypeInteger, "1; DROP TABLE users"},
		{domain.WatermarkTypeFloat, "NaN"},
		{domain.WatermarkTypeFloat, "+Inf"},
		{domain.WatermarkTypeTimestamp, "24/10/2026"},
		{domain.WatermarkTypeDate, "2026-13-40"},
		{"unsupported_type", "123"},
	}

	for _, tc := range cases {
		_, err := domain.ParseWatermarkValue(tc.t, tc.val)
		if err == nil {
			t.Errorf("expected error for type %q value %q, got nil", tc.t, tc.val)
		}
	}
}
