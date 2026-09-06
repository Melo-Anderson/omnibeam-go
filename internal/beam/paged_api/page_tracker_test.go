// package paged_api validates the Splittable DoFn restriction trackers.
package paged_api

import (
	"testing"
)

func TestPageRangeTracker_Claim(t *testing.T) {
	tests := []struct {
		name         string
		rest         PageRange
		claims       []int
		wantDone     bool
		wantProgress []float64
	}{
		{
			name:         "claim all pages sequentially",
			rest:         PageRange{Start: 0, End: 2},
			claims:       []int{0, 1},
			wantDone:     true,
			wantProgress: []float64{2, 0},
		},
		{
			name:         "claim out of bounds",
			rest:         PageRange{Start: 0, End: 2},
			claims:       []int{5},
			wantDone:     true, // stops on invalid claim
			wantProgress: []float64{0, 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewPageRangeTracker(tc.rest)
			for _, c := range tc.claims {
				tracker.TryClaim(c)
			}
			if got := tracker.IsDone(); got != tc.wantDone {
				t.Errorf("IsDone() = %v, want %v", got, tc.wantDone)
			}
			done, remaining := tracker.GetProgress()
			if done != tc.wantProgress[0] || remaining != tc.wantProgress[1] {
				t.Errorf("GetProgress() = %v, %v; want %v, %v", done, remaining, tc.wantProgress[0], tc.wantProgress[1])
			}
		})
	}
}

func TestPageRangeTracker_TrySplit(t *testing.T) {
	tests := []struct {
		name      string
		rest      PageRange
		claims    []int
		fraction  float64
		wantPrim  PageRange
		wantResid PageRange
		wantSplit bool
	}{
		{
			name:      "split midway",
			rest:      PageRange{Start: 0, End: 10},
			claims:    []int{0, 1},
			fraction:  0.5,
			wantPrim:  PageRange{Start: 0, End: 6},
			wantResid: PageRange{Start: 6, End: 10},
			wantSplit: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewPageRangeTracker(tc.rest)
			for _, c := range tc.claims {
				tracker.TryClaim(c)
			}
			prim, resid, err := tracker.TrySplit(tc.fraction)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSplit {
				if prim == nil || resid == nil {
					t.Fatal("expected non-nil split results")
				}
				if prim.(PageRange) != tc.wantPrim || resid.(PageRange) != tc.wantResid {
					t.Errorf("TrySplit() = %v, %v; want %v, %v", prim, resid, tc.wantPrim, tc.wantResid)
				}
			}
		})
	}
}

func TestPageRangeTracker_ProgressAndErrors(t *testing.T) {
	rest := PageRange{Start: 0, End: 10}
	tracker := NewPageRangeTracker(rest)

	if tracker.GetRestriction() != rest {
		t.Errorf("GetRestriction mismatch: got %v, want %v", tracker.GetRestriction(), rest)
	}

	// TryClaim with invalid type
	if tracker.TryClaim("invalid") {
		t.Error("expected TryClaim with string to fail")
	}
	if tracker.GetError() == nil {
		t.Error("expected error after invalid claim type")
	}

	// TryClaim with int64
	t2 := NewPageRangeTracker(rest)
	if !t2.TryClaim(int64(2)) {
		t.Error("expected TryClaim with int64 to succeed")
	}

	// Invalid fractions
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
}
