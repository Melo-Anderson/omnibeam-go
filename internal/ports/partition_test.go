// Package ports_test validates the abstract partitioning and paged API contracts.
package ports_test

import (
	"encoding/json"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestPartitionSlice_JSONSerialization(t *testing.T) {
	tests := []struct {
		name  string
		input ports.PartitionSlice
	}{
		{
			name: "valid serialization",
			input: ports.PartitionSlice{
				SliceIndex: 1,
				LowerBound: 100,
				UpperBound: 200,
				IsFirst:    false,
				IsLast:     true,
				Metadata: map[string]string{
					"shard_id": "shard-01",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.input)
			if err != nil {
				t.Fatalf("failed to marshal PartitionSlice: %v", err)
			}

			var got ports.PartitionSlice
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("failed to unmarshal PartitionSlice: %v", err)
			}

			if got.SliceIndex != tc.input.SliceIndex || got.IsFirst != tc.input.IsFirst || got.IsLast != tc.input.IsLast {
				t.Errorf("unexpected decoded slice: %+v", got)
			}
			if got.Metadata["shard_id"] != tc.input.Metadata["shard_id"] {
				t.Errorf("expected shard_id metadata 'shard-01', got: %v", got.Metadata["shard_id"])
			}
		})
	}
}

func TestPageSlice_JSONSerialization(t *testing.T) {
	tests := []struct {
		name  string
		input ports.PageSlice
	}{
		{
			name: "valid serialization",
			input: ports.PageSlice{
				PageIndex: 3,
				PageSize:  50,
				Cursor:    "cursor_xyz_123",
				Metadata: map[string]string{
					"next_url": "https://api.example.com/v1/records?page=4",
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.input)
			if err != nil {
				t.Fatalf("failed to marshal PageSlice: %v", err)
			}

			var got ports.PageSlice
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("failed to unmarshal PageSlice: %v", err)
			}

			if got.PageIndex != tc.input.PageIndex || got.PageSize != tc.input.PageSize || got.Cursor != tc.input.Cursor {
				t.Errorf("unexpected decoded page: %+v", got)
			}
			if got.Metadata["next_url"] != tc.input.Metadata["next_url"] {
				t.Errorf("expected next_url metadata, got: %v", got.Metadata["next_url"])
			}
		})
	}
}
