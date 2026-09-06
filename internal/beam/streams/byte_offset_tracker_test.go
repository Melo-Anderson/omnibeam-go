// package streams validates the ByteOffsetTracker restriction lifecycle.
package streams

import (
	"bytes"
	"testing"
)

func TestByteOffsetTracker_ClaimAndSplit(t *testing.T) {
	tests := []struct {
		name           string
		restriction    ByteOffsetRange
		claimAt        int64
		splitFraction  float64
		wantPrimaryEnd int64
	}{
		{
			name:           "claim mid-range and split at 0.5",
			restriction:    ByteOffsetRange{Start: 0, End: 1000},
			claimAt:        100,
			splitFraction:  0.5,
			wantPrimaryEnd: 550,
		},
		{
			name:           "claim near end gives small residual",
			restriction:    ByteOffsetRange{Start: 0, End: 100},
			claimAt:        90,
			splitFraction:  0.5,
			wantPrimaryEnd: 95,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewByteOffsetTracker(tc.restriction)

			if !tracker.TryClaim(tc.claimAt) {
				t.Fatalf("TryClaim(%d) = false, want true", tc.claimAt)
			}
			if tracker.IsDone() {
				t.Fatal("IsDone() = true, want false after single claim")
			}

			primaryAny, residualAny, err := tracker.TrySplit(tc.splitFraction)
			if err != nil || primaryAny == nil {
				t.Fatalf("TrySplit failed: err=%v, primary=%v", err, primaryAny)
			}
			primary := primaryAny.(ByteOffsetRange)
			residual := residualAny.(ByteOffsetRange)
			if primary.End != residual.Start {
				t.Errorf("split boundaries mismatch: primary.End=%d, residual.Start=%d", primary.End, residual.Start)
			}
			if primary.End != tc.wantPrimaryEnd {
				t.Errorf("primary.End = %d, want %d", primary.End, tc.wantPrimaryEnd)
			}
		})
	}
}

func TestByteOffsetTracker_IsDoneAfterFullClaim(t *testing.T) {
	rest := ByteOffsetRange{Start: 0, End: 10}
	tracker := NewByteOffsetTracker(rest)

	_ = tracker.TryClaim(9)
	if !tracker.IsDone() {
		t.Error("IsDone() = false, want true after claiming last byte")
	}
}

func TestByteOffsetTracker_ClaimOutOfRangeFails(t *testing.T) {
	rest := ByteOffsetRange{Start: 0, End: 100}
	tracker := NewByteOffsetTracker(rest)

	if tracker.TryClaim(100) {
		t.Error("TryClaim(100) = true, want false (end is exclusive)")
	}
	if tracker.TryClaim(-1) {
		t.Error("TryClaim(-1) = true, want false")
	}
}

func TestSyncToNextNewline(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		startOffset int64
		wantOffset  int64
	}{
		{
			name:        "skip to newline mid-line",
			data:        []byte("first line\nsecond line\nthird line\n"),
			startOffset: 3,
			wantOffset:  11,
		},
		{
			name:        "offset zero returns zero (first chunk, no skip)",
			data:        []byte("first line\nsecond line\n"),
			startOffset: 0,
			wantOffset:  0,
		},
		{
			name:        "no newline returns end of data",
			data:        []byte("no newline here"),
			startOffset: 5,
			wantOffset:  int64(len("no newline here")),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SyncToNextNewline(tc.data, tc.startOffset)
			if got != tc.wantOffset {
				t.Errorf("SyncToNextNewline offset = %d, want %d", got, tc.wantOffset)
			}
		})
	}
}

func TestByteOffsetTracker_ProgressAndErrors(t *testing.T) {
	rest := ByteOffsetRange{Start: 10, End: 100}
	tracker := NewByteOffsetTracker(rest)

	done, remaining := tracker.GetProgress()
	if done != 0 || remaining != 90 {
		t.Errorf("initial progress: done=%f, remaining=%f, want 0, 90", done, remaining)
	}

	// TryClaim with invalid type
	if tracker.TryClaim("invalid") {
		t.Error("expected TryClaim with string to fail")
	}
	if tracker.GetError() == nil {
		t.Error("expected non-nil error after invalid claim type")
	}

	// TryClaim int
	t2 := NewByteOffsetTracker(rest)
	if !t2.TryClaim(20) { // int type
		t.Error("expected TryClaim(20) with int to succeed")
	}
	done2, remaining2 := t2.GetProgress()
	if done2 != 11 || remaining2 != 79 {
		t.Errorf("progress after claim(20): done=%f, remaining=%f", done2, remaining2)
	}

	// TrySplit with invalid fractions
	p, r, err := t2.TrySplit(0.0)
	if p != nil || r != nil || err != nil {
		t.Error("expected nil split for fraction <= 0")
	}
	p, r, err = t2.TrySplit(1.0)
	if p != nil || r != nil || err != nil {
		t.Error("expected nil split for fraction >= 1")
	}

	// MarkDone
	t2.MarkDone()
	if !t2.IsDone() {
		t.Error("expected IsDone() = true after MarkDone()")
	}
	p, r, err = t2.TrySplit(0.5)
	if p != nil || r != nil {
		t.Error("expected nil split after tracker stopped")
	}
}

func TestSyncToNextRecordBoundary_QuotedMultiline(t *testing.T) {
	csvData := []byte("101,\"Address Line 1\nApartment 2B\",200\n102,\"Standard Name\",300\n")

	// If a worker split starts at offset 15 (inside the quoted address):
	// It should NOT stop at the '\n' at offset 20.
	// It MUST stop at the start of record 102.
	syncPos := SyncToNextRecordBoundary(csvData, 15, '"')
	expectedPos := int64(bytes.Index(csvData, []byte("102,")))

	if syncPos != expectedPos {
		t.Fatalf("expected sync position %d (start of record 102), got %d (snippet: %q)", expectedPos, syncPos, string(csvData[syncPos:]))
	}
}

func TestByteOffsetTracker_ClaimOffset(t *testing.T) {
	tracker := NewByteOffsetTracker(ByteOffsetRange{Start: 10, End: 50})
	if !tracker.ClaimOffset(10) {
		t.Error("expected ClaimOffset(10) to succeed")
	}
	if !tracker.ClaimOffset(20) {
		t.Error("expected ClaimOffset(20) to succeed")
	}
	if tracker.ClaimOffset(20) {
		t.Error("expected duplicate ClaimOffset(20) to fail")
	}
	if !tracker.IsDone() {
		t.Error("expected tracker to be stopped/done after invalid claim")
	}
}

func TestSyncToNextRecordBoundary_EscapedQuotesWithNewlines(t *testing.T) {
	// Row 1: "101","Hello ""World""\nMulti-line",200\n
	// Row 2: "102","Simple",300\n
	data := []byte("\"101\",\"Hello \"\"World\"\"\nMulti-line\",200\n\"102\",\"Simple\",300\n")

	// Splitting anywhere in Row 1 must sync to Row 2 start when multiline is true
	boundary := SyncToNextRecordBoundary(data, 10, '"', true)
	expectedStart := int64(len("\"101\",\"Hello \"\"World\"\"\nMulti-line\",200\n"))
	if boundary != expectedStart {
		t.Fatalf("expected boundary at %d, got %d", expectedStart, boundary)
	}

	// When multiline is false, fast-path syncs to first newline
	fastBoundary := SyncToNextRecordBoundary(data, 10, '"', false)
	firstNewline := int64(bytes.IndexByte(data[10:], '\n') + 10 + 1)
	if fastBoundary != firstNewline {
		t.Fatalf("expected fast-path boundary at %d, got %d", firstNewline, fastBoundary)
	}
}

func TestByteOffsetTracker_SubMegabyteLiquidSplits(t *testing.T) {
	// 5 MB restriction
	rest := ByteOffsetRange{Start: 0, End: 5 * 1024 * 1024}
	tracker := NewByteOffsetTracker(rest)

	// Claim first 1 MB
	if !tracker.TryClaim(1 * 1024 * 1024) {
		t.Fatal("expected claim to succeed")
	}

	// Dynamic split requested at 50% of remaining (remaining is 1MB..5MB -> split at ~3MB)
	primary, residual, err := tracker.TrySplit(0.5)
	if err != nil || primary == nil || residual == nil {
		t.Fatal("expected TrySplit to succeed")
	}

	primRange := primary.(ByteOffsetRange)
	resRange := residual.(ByteOffsetRange)

	if primRange.End != resRange.Start {
		t.Errorf("primary end (%d) must equal residual start (%d)", primRange.End, resRange.Start)
	}
	if resRange.End != rest.End {
		t.Errorf("residual end (%d) must equal original end (%d)", resRange.End, rest.End)
	}

	// Verify progress reporting accuracy
	done, remaining := tracker.GetProgress()
	if done <= 0 || remaining <= 0 {
		t.Errorf("expected positive progress, got done=%f remaining=%f", done, remaining)
	}
}



