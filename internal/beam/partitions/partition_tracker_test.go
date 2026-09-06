// package partitions validates the PartitionRangeTracker restriction lifecycle.
package partitions

import (
	"testing"

	)

func TestPartitionRangeTracker_ClaimAndSplit(t *testing.T) {
	tests := []struct {
		name           string
		restriction    PartitionRange
		claimSlices    []int
		splitFraction  float64
		wantPrimaryEnd int
	}{
		{
			name:           "claim two slices and split at 0.5 over 10 slices",
			restriction:    PartitionRange{Start: 0, End: 10},
			claimSlices:    []int{0, 1},
			splitFraction:  0.5,
			wantPrimaryEnd: 6, // 1 + 1 + round((10-1-1)*0.5) = 6
		},
		{
			name:           "claim first slice only, split at 0.5 over 4 slices",
			restriction:    PartitionRange{Start: 0, End: 4},
			claimSlices:    []int{0},
			splitFraction:  0.5,
			wantPrimaryEnd: 3, // 0 + 1 + round((4-0-1)*0.5) = 1 + 2 = 3
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewPartitionRangeTracker(tc.restriction)

			for _, idx := range tc.claimSlices {
				if !tracker.TryClaim(idx) {
					t.Fatalf("TryClaim(%d) = false, want true", idx)
				}
			}

			primaryAny, residualAny, err := tracker.TrySplit(tc.splitFraction)
			if err != nil || primaryAny == nil {
				t.Fatalf("TrySplit failed: err=%v, primary=%v", err, primaryAny)
			}
			primary := primaryAny.(PartitionRange)
			residual := residualAny.(PartitionRange)
			if primary.End != residual.Start {
				t.Errorf("split mismatch: primary.End=%d, residual.Start=%d", primary.End, residual.Start)
			}
			if primary.End != tc.wantPrimaryEnd {
				t.Errorf("primary.End = %d, want %d", primary.End, tc.wantPrimaryEnd)
			}
		})
	}
}

func TestPartitionRangeTracker_IsDoneAfterLastSlice(t *testing.T) {
	rest := PartitionRange{Start: 0, End: 3}
	tracker := NewPartitionRangeTracker(rest)

	_ = tracker.TryClaim(2)
	if !tracker.IsDone() {
		t.Error("IsDone() = false, want true after claiming last slice index")
	}
}

func TestPartitionRangeTracker_ClaimOutOfRangeFails(t *testing.T) {
	rest := PartitionRange{Start: 0, End: 5}
	tracker := NewPartitionRangeTracker(rest)

	if tracker.TryClaim(5) {
		t.Error("TryClaim(5) = true, want false (end is exclusive)")
	}
	if tracker.TryClaim(-1) {
		t.Error("TryClaim(-1) = true, want false")
	}
}

func TestPartitionRangeTracker_ProgressAndErrors(t *testing.T) {
	rest := PartitionRange{Start: 0, End: 10}
	tracker := NewPartitionRangeTracker(rest)

	if tracker.GetRestriction() != rest {
		t.Errorf("GetRestriction mismatch: got %v, want %v", tracker.GetRestriction(), rest)
	}

	done, remaining := tracker.GetProgress()
	if done != 0 || remaining != 10 {
		t.Errorf("initial progress: done=%f, remaining=%f, want 0, 10", done, remaining)
	}

	// TryClaim with invalid type
	if tracker.TryClaim("invalid") {
		t.Error("expected TryClaim with string to fail")
	}
	if tracker.GetError() == nil {
		t.Error("expected error after invalid claim type")
	}

	// TryClaim with int64
	t2 := NewPartitionRangeTracker(rest)
	if !t2.TryClaim(int64(3)) {
		t.Error("expected TryClaim with int64 to succeed")
	}
	done2, remaining2 := t2.GetProgress()
	if done2 != 4 || remaining2 != 6 {
		t.Errorf("progress after claim(3): done=%f, remaining=%f", done2, remaining2)
	}

	// Invalid split fractions
	p, r, err := t2.TrySplit(0.0)
	if p != nil || r != nil || err != nil {
		t.Error("expected nil split for fraction <= 0")
	}
	p, r, err = t2.TrySplit(1.0)
	if p != nil || r != nil || err != nil {
		t.Error("expected nil split for fraction >= 1")
	}
}

func TestPartitionRangeTracker_SubRangeSplits(t *testing.T) {
	// Large discrete partition slice range: 100 partitions
	rest := PartitionRange{Start: 0, End: 100}
	tracker := NewPartitionRangeTracker(rest)

	// Claim partition 20
	if !tracker.TryClaim(20) {
		t.Fatal("expected claim to succeed")
	}

	// Request dynamic split at 50%
	primary, residual, err := tracker.TrySplit(0.5)
	if err != nil || primary == nil || residual == nil {
		t.Fatal("expected TrySplit to succeed")
	}

	primRange := primary.(PartitionRange)
	resRange := residual.(PartitionRange)

	if primRange.End != resRange.Start {
		t.Errorf("primary end (%d) must match residual start (%d)", primRange.End, resRange.Start)
	}
	if resRange.End != rest.End {
		t.Errorf("residual end (%d) must match rest end (%d)", resRange.End, rest.End)
	}

	done, remaining := tracker.GetProgress()
	if done <= 0 || remaining <= 0 {
		t.Errorf("expected positive progress, got done=%f remaining=%f", done, remaining)
	}
}

