// package partitions provides Splittable DoFn (SDF) implementations for partitioned datastore reading.
// DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
package partitions

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*PartitionRange)(nil)).Elem())
	beam.RegisterDoFn(reflect.TypeOf((*PartitionQuerySourceSDF)(nil)).Elem())
	// NOTE: ports.PartitionSlice coder is registered canonically in
	// internal/beam/core/graph_builder.go to avoid duplicate registration panic.
}

// PartitionQuerySourceSDF is a generic Splittable DoFn that reads discrete partitioned query slices.
type PartitionQuerySourceSDF struct {
	DBCfg      *domain.DatabaseSourceConfig `json:"db_cfg"`
	SecretsCfg *domain.SecretsConfig        `json:"secrets_cfg,omitempty"`
	reader     ports.PartitionedReader
}

// NewPartitionQuerySourceSDF creates a new PartitionQuerySourceSDF DoFn.
// Pass a non-nil reader to bypass the provider (e.g., in unit tests).
// Pass nil reader to rely on Setup() initializing via globalPartitionedReaderProvider.
func NewPartitionQuerySourceSDF(reader ports.PartitionedReader, dbCfg *domain.DatabaseSourceConfig, secCfg *domain.SecretsConfig) *PartitionQuerySourceSDF {
	return &PartitionQuerySourceSDF{
		reader:     reader,
		DBCfg:      dbCfg,
		SecretsCfg: secCfg,
	}
}

// Setup is called once per DoFn bundle on each worker to initialize the connection pool.
// Signature: func(ctx context.Context) error — valid per Beam Go SDK (fn.go validation).
func (fn *PartitionQuerySourceSDF) Setup(ctx context.Context) error {
	if fn.reader != nil {
		// Pre-injected reader (unit test path or direct runner with connection sharing).
		return nil
	}
	if globalPartitionedReaderProvider == nil {
		return fmt.Errorf("PartitionQuerySourceSDF: globalPartitionedReaderProvider not registered — call beam.RegisterInit in an init() before beam.Init()")
	}
	reader, err := globalPartitionedReaderProvider(ctx, fn.DBCfg, fn.SecretsCfg)
	if err != nil {
		return fmt.Errorf("PartitionQuerySourceSDF.Setup: failed initializing reader for driver %q: %w", fn.DBCfg.Driver, err)
	}
	fn.reader = reader
	return nil
}

// Teardown is called when the DoFn bundle completes, releasing the connection pool.
// Signature: func() error — valid per Beam Go SDK (fn.go validation).
func (fn *PartitionQuerySourceSDF) Teardown() error {
	if fn.reader != nil {
		return fn.reader.Close()
	}
	return nil
}

// CreateInitialRestriction creates the initial partition range restriction for a single slice.
func (fn *PartitionQuerySourceSDF) CreateInitialRestriction(_ context.Context, _ ports.PartitionSlice) (PartitionRange, error) {
	return PartitionRange{Start: 0, End: 1}, nil
}

// SplitRestriction returns the single slice restriction (SDF does not sub-split partitions).
func (fn *PartitionQuerySourceSDF) SplitRestriction(_ ports.PartitionSlice, rest PartitionRange) []PartitionRange {
	return []PartitionRange{rest}
}

// RestrictionSize computes the estimated row count of the partition range.
// Weighted by BatchSize so the Dataflow Autoscaler receives accurate backlog estimates.
func (fn *PartitionQuerySourceSDF) RestrictionSize(_ ports.PartitionSlice, rest PartitionRange) float64 {
	if rest.End < rest.Start {
		return 0
	}
	batchSize := float64(domain.DefaultDatabaseBatchSize)
	if fn.DBCfg != nil && fn.DBCfg.PartitionConfig.BatchSize > 0 {
		batchSize = float64(fn.DBCfg.PartitionConfig.BatchSize)
	}
	return batchSize * float64(rest.End-rest.Start)
}

// CreateTracker returns a new PartitionRangeTracker initialized with the given restriction.
func (fn *PartitionQuerySourceSDF) CreateTracker(rest PartitionRange) *PartitionRangeTracker {
	return NewPartitionRangeTracker(rest)
}

// ProcessElement executes the partition query slice and emits records downstream.
// The reader must already be initialized by Setup() — no lazy init in the hot path.
func (fn *PartitionQuerySourceSDF) ProcessElement(
	ctx context.Context,
	tracker *PartitionRangeTracker,
	slice ports.PartitionSlice,
	emit func(*domain.GenericRecord),
) error {
	if !tracker.TryClaim(0) {
		return nil
	}

	if fn.reader == nil {
		return fmt.Errorf("PartitionQuerySourceSDF: reader not initialized — Setup() must run before ProcessElement")
	}

	recChan, errChan, err := fn.reader.ReadPartition(ctx, fn.DBCfg, slice)
	if err != nil {
		return fmt.Errorf("failed reading partition slice %d: %w", slice.SliceIndex, err)
	}

	for rec := range recChan {
		if rec != nil {
			emit(rec)
		}
	}

	for err := range errChan {
		if err != nil {
			return fmt.Errorf("error in partition slice %d: %w", slice.SliceIndex, err)
		}
	}

	return nil
}
