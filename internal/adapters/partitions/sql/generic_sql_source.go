// Package sql implements relational database ingestion adapters and universal range slicing.
package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"github.com/omnibeam/dataflow-compute-go/pkg/resilience"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Compile-time assertion that GenericSQLSource satisfies ports.SQLReader and ports.PartitionedReader (LSP).
var _ ports.SQLReader = (*GenericSQLSource)(nil)
var _ ports.PartitionedReader = (*GenericSQLSource)(nil)

// GenericSQLSource is a universal SQL adapter executing analytical window range slicing
// and concurrent partition reading across any relational database with CircuitBreaker protection.
type GenericSQLSource struct {
	db            *sql.DB
	isDollarStyle bool
	cb            *resilience.CircuitBreaker
}

// NewGenericSQLSource constructs a SQLReader adapter with the designated parameter syntax.
func NewGenericSQLSource(db *sql.DB, isDollarStyle bool) *GenericSQLSource {
	cb, _ := resilience.NewCircuitBreaker(resilience.Settings{
		Name:        "sql-source-cb",
		MaxFailures: domain.DefaultDatabaseCircuitBreakerFails,
		Timeout:     time.Duration(domain.DefaultCircuitBreakerTimeoutMs) * time.Millisecond,
	})
	return &GenericSQLSource{
		db:            db,
		isDollarStyle: isDollarStyle,
		cb:            cb,
	}
}

// WithCircuitBreaker injects a custom circuit breaker instance (DIP).
func (s *GenericSQLSource) WithCircuitBreaker(cb *resilience.CircuitBreaker) *GenericSQLSource {
	s.cb = cb
	return s
}

// NewPostgresSource returns a SQLReader configured with PostgreSQL $1 parameter syntax.
func NewPostgresSource(db *sql.DB) ports.SQLReader {
	return NewGenericSQLSource(db, true)
}

// NewMySQLSource returns a SQLReader configured with MySQL ? parameter syntax.
func NewMySQLSource(db *sql.DB) ports.SQLReader {
	return NewGenericSQLSource(db, false)
}

func (s *GenericSQLSource) queryWithCircuitBreaker(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database connection is not initialized")
	}

	var rows *sql.Rows
	var qErr error

	if s.cb != nil {
		qErr = s.cb.Execute(func() error {
			var err error
			rows, err = s.db.QueryContext(ctx, query, args...)
			return err
		})
	} else {
		rows, qErr = s.db.QueryContext(ctx, query, args...)
	}

	return rows, qErr
}

// CalculateSlices queries the source database to calculate balanced query partition slices.
func (s *GenericSQLSource) CalculateSlices(ctx context.Context, cfg *domain.DatabaseSourceConfig) ([]ports.SQLSlice, error) {
	if s.cb != nil && s.cb.State() == resilience.StateOpen {
		return nil, resilience.ErrCircuitOpen
	}

	tracer := otel.Tracer("sql-source")
	ctx, span := tracer.Start(ctx, "GenericSQLSource.CalculateSlices")
	defer span.End()

	col := strings.TrimSpace(cfg.PartitionConfig.PartitionColumn)
	span.SetAttributes(
		attribute.String("db.partition_column", col),
		attribute.Int("db.batch_size", cfg.PartitionConfig.BatchSize),
	)

	if col == "" {
		span.SetAttributes(attribute.Bool("db.single_slice", true))
		return []ports.SQLSlice{
			{SliceIndex: 0, IsFirst: true, IsLast: true},
		}, nil
	}

	boundsQuery := BuildUniversalBoundsQuery(cfg.BaseQuery(), col, cfg.PartitionConfig.BatchSize)
	rows, qErr := s.queryWithCircuitBreaker(ctx, boundsQuery)
	if qErr != nil {
		span.RecordError(qErr)
		span.SetStatus(codes.Error, qErr.Error())
		return nil, fmt.Errorf("failed executing universal bounds query: %w", qErr)
	}
	defer rows.Close()

	buckets, err := scanRawBuckets(rows)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	slices := ConstructContiguousSlices(buckets)
	span.SetAttributes(attribute.Int("db.slice_count", len(slices)))
	return slices, nil
}

func scanRawBuckets(rows *sql.Rows) ([]RawBucket, error) {
	var buckets []RawBucket
	for rows.Next() {
		var (
			bucketID int
			lower    any
			upper    any
			rowCount int64
		)
		if err := rows.Scan(&bucketID, &lower, &upper, &rowCount); err != nil {
			return nil, fmt.Errorf("failed scanning bounds row: %w", err)
		}
		buckets = append(buckets, RawBucket{
			BucketID: bucketID,
			Lower:    lower,
			Upper:    upper,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("bounds rows iteration error: %w", err)
	}
	return buckets, nil
}

// ReadSlice executes the bounded query for a single worker slice and streams parsed records and errors.
func (s *GenericSQLSource) ReadSlice(ctx context.Context, cfg *domain.DatabaseSourceConfig, slice ports.SQLSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	if s.cb != nil && s.cb.State() == resilience.StateOpen {
		return nil, nil, resilience.ErrCircuitOpen
	}

	tracer := otel.Tracer("sql-source")
	ctx, span := tracer.Start(ctx, "GenericSQLSource.ReadSlice")
	span.SetAttributes(
		attribute.Int("db.slice_index", slice.SliceIndex),
		attribute.String("db.lower_bound", fmt.Sprintf("%v", slice.LowerBound)),
		attribute.String("db.upper_bound", fmt.Sprintf("%v", slice.UpperBound)),
	)
	defer span.End()

	col := strings.TrimSpace(cfg.PartitionConfig.PartitionColumn)
	query, args := BuildWorkerSliceQuery(cfg.BaseQuery(), col, slice, s.isDollarStyle)

	rows, qErr := s.queryWithCircuitBreaker(ctx, query, args...)
	if qErr != nil {
		span.RecordError(qErr)
		span.SetStatus(codes.Error, qErr.Error())
		return nil, nil, fmt.Errorf("failed executing slice query: %w", qErr)
	}

	recChan := make(chan *domain.GenericRecord, 100)
	errChan := make(chan error, 100)

	go streamRows(rows, &cfg.Schema, recChan, errChan)

	return recChan, errChan, nil
}

func streamRows(rows *sql.Rows, schema *domain.Schema, recChan chan<- *domain.GenericRecord, errChan chan<- error) {
	defer rows.Close()
	defer close(recChan)
	defer close(errChan)

	holder := NewScanTargetHolder(schema)
	for rows.Next() {
		if err := rows.Scan(holder.GetScanTargets()...); err != nil {
			errChan <- fmt.Errorf("scan error: %w", err)
			continue
		}
		rec, err := holder.ExtractRecord(schema)
		if err != nil {
			errChan <- err
			continue
		}
		recChan <- rec
	}
	if err := rows.Err(); err != nil {
		errChan <- fmt.Errorf("rows iteration error: %w", err)
	}
}

// CalculatePartitions implements ports.PartitionedReader for relational databases.
func (s *GenericSQLSource) CalculatePartitions(ctx context.Context, srcCfg any) ([]ports.PartitionSlice, error) {
	cfg, ok := srcCfg.(*domain.DatabaseSourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type %T for SQL partitioned reader, expected *domain.DatabaseSourceConfig", srcCfg)
	}
	return s.CalculateSlices(ctx, cfg)
}

// ReadPartition implements ports.PartitionedReader for relational databases.
func (s *GenericSQLSource) ReadPartition(ctx context.Context, srcCfg any, slice ports.PartitionSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	cfg, ok := srcCfg.(*domain.DatabaseSourceConfig)
	if !ok {
		return nil, nil, fmt.Errorf("invalid config type %T for SQL partitioned reader, expected *domain.DatabaseSourceConfig", srcCfg)
	}
	return s.ReadSlice(ctx, cfg, slice)
}

// Close safely closes the underlying database connection pool.
func (s *GenericSQLSource) Close() error {
	if s.db != nil {
		return CloseDB(s.db)
	}
	return nil
}
