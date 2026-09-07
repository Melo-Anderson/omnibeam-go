// Package sql implements relational database ingestion adapters and range slicing.
package sql

import (
	"fmt"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// RawBucket captures the minimum and maximum boundaries of a single ranked window partition.
type RawBucket struct {
	BucketID int
	Lower    any
	Upper    any
}

// BuildUniversalBoundsQuery constructs an outer analytical query wrapping baseQuery
// to dynamically compute balanced partition ranges across any database and key type.
func BuildUniversalBoundsQuery(baseQuery, partitionCol string, batchSize int) string {
	trimmed := strings.TrimSpace(baseQuery)
	return fmt.Sprintf(
		"SELECT bucket_id, MIN(%s) AS lower_bound, MAX(%s) AS upper_bound, COUNT(*) AS row_count FROM (SELECT %s, CEIL(ROW_NUMBER() OVER (ORDER BY %s) / %d.0) AS bucket_id FROM (%s) AS _user_subq) AS _ranked GROUP BY bucket_id ORDER BY bucket_id",
		partitionCol, partitionCol, partitionCol, partitionCol, batchSize, trimmed,
	)
}

// BuildWorkerSliceQuery constructs the isolated query for a single worker slice.
// The last slice uses an open-ended right boundary (>= lower only) to capture
// rows inserted concurrently during pipeline execution — preventing silent data loss.
func BuildWorkerSliceQuery(baseQuery, partitionCol string, slice ports.SQLSlice, isDollarStyle bool) (string, []any) {
	trimmed := strings.TrimSpace(baseQuery)

	// Single-partition pipelines or nil-bound slices: full scan, no predicates.
	if (slice.IsFirst && slice.IsLast) || (slice.LowerBound == nil && slice.UpperBound == nil) {
		return fmt.Sprintf("SELECT * FROM (%s) AS _user_subq", trimmed), nil
	}

	p1, p2 := "$1", "$2"
	if !isDollarStyle {
		p1, p2 = "?", "?"
	}

	// Last slice: open right boundary captures rows inserted after discovery.
	if slice.IsLast {
		query := fmt.Sprintf(
			"SELECT * FROM (%s) AS _user_subq WHERE %s >= %s ORDER BY %s",
			trimmed, partitionCol, p1, partitionCol,
		)
		return query, []any{slice.LowerBound}
	}

	// Intermediate slices: semi-open interval [lower, upper).
	query := fmt.Sprintf(
		"SELECT * FROM (%s) AS _user_subq WHERE %s >= %s AND %s < %s ORDER BY %s",
		trimmed, partitionCol, p1, partitionCol, p2, partitionCol,
	)
	return query, []any{slice.LowerBound, slice.UpperBound}
}

// ConstructContiguousSlices transforms raw window buckets into contiguous SQLSlice instances,
// ensuring zero row drops and zero duplicates across bucket boundaries.
func ConstructContiguousSlices(buckets []RawBucket) []ports.SQLSlice {
	if len(buckets) == 0 {
		return []ports.SQLSlice{
			{SliceIndex: 0, IsFirst: true, IsLast: true},
		}
	}

	slices := make([]ports.SQLSlice, len(buckets))
	for i := range buckets {
		isFirst := i == 0
		isLast := i == len(buckets)-1

		lower := buckets[i].Lower
		var upper any
		if isLast {
			upper = buckets[i].Upper
		} else {
			upper = buckets[i+1].Lower
		}

		slices[i] = ports.SQLSlice{
			SliceIndex: i,
			LowerBound: lower,
			UpperBound: upper,
			IsFirst:    isFirst,
			IsLast:     isLast,
		}
	}
	return slices
}
