package mongo_test

import (
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/mongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBuildObjectIDRanges(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 1, 4, 0, 0, 0, time.UTC)

	minID := bson.NewObjectIDFromTimestamp(t0)
	maxID := bson.NewObjectIDFromTimestamp(t1)

	t.Run("Splits into 4 evenly distributed time intervals", func(t *testing.T) {
		slices := mongo.BuildObjectIDRanges(minID, maxID, 4)
		if len(slices) != 4 {
			t.Fatalf("expected 4 slices, got %d", len(slices))
		}

		if !slices[0].IsFirst {
			t.Errorf("slice 0 must be marked IsFirst")
		}
		if !slices[3].IsLast {
			t.Errorf("slice 3 must be marked IsLast")
		}

		for i := 0; i < len(slices)-1; i++ {
			if slices[i].UpperBound != slices[i+1].LowerBound {
				t.Errorf("slice %d upper bound (%v) does not match slice %d lower bound (%v)",
					i, slices[i].UpperBound, i+1, slices[i+1].LowerBound)
			}
		}
	})

	t.Run("Single partition or zero returns whole range", func(t *testing.T) {
		slices := mongo.BuildObjectIDRanges(minID, maxID, 1)
		if len(slices) != 1 || !slices[0].IsFirst || !slices[0].IsLast {
			t.Errorf("expected 1 slice marked first and last")
		}

		slicesZero := mongo.BuildObjectIDRanges(minID, maxID, 0)
		if len(slicesZero) != 1 {
			t.Errorf("expected 1 slice for 0 partitions")
		}
	})

	t.Run("Inverted timestamps return single slice", func(t *testing.T) {
		slices := mongo.BuildObjectIDRanges(maxID, minID, 4)
		if len(slices) != 1 {
			t.Errorf("expected 1 slice for inverted timestamps, got %d", len(slices))
		}
	})

	t.Run("Close timestamps produce minimal step", func(t *testing.T) {
		tClose := t0.Add(2 * time.Second)
		maxClose := bson.NewObjectIDFromTimestamp(tClose)
		slices := mongo.BuildObjectIDRanges(minID, maxClose, 5)
		if len(slices) != 5 {
			t.Errorf("expected 5 slices for close timestamps, got %d", len(slices))
		}
	})
}
