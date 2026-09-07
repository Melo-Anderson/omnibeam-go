package functional_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	sql_adapter "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/sql"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/partitions"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/streams"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

type parquetRowRecord struct {
	ID         int64     `parquet:"id"`
	CustomerID string    `parquet:"customer_id"`
	Amount     float64   `parquet:"amount"`
	Status     string    `parquet:"status"`
	CreatedAt  time.Time `parquet:"created_at"`
}

func TestSDF_DynamicWorkRebalance_5Million(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 5M scale test in short mode")
	}

	ctx := context.Background()
	pgHost := os.Getenv("PGHOST")
	if pgHost == "" {
		pgHost = "localhost"
	}
	pgPort := os.Getenv("PGPORT")
	if pgPort == "" {
		pgPort = "5432"
	}
	pgUser := os.Getenv("PGUSER")
	if pgUser == "" {
		pgUser = "postgres"
	}
	pgPassword := os.Getenv("PGPASSWORD")
	if pgPassword == "" {
		pgPassword = "root"
	}
	pgDatabase := os.Getenv("PGDATABASE")
	if pgDatabase == "" {
		pgDatabase = "benchmark"
	}

	connURI := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pgUser, pgPassword, pgHost, pgPort, pgDatabase)

	db, err := sql.Open("pgx", connURI)
	if err != nil {
		t.Fatalf("failed connecting to postgres: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("skipping 5M scale test: postgres database unreachable (%v)", err)
	}

	var totalRows int64
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM orders_int").Scan(&totalRows); err != nil {
		t.Fatalf("failed querying row count: %v", err)
	}
	if totalRows < 5000000 {
		t.Skipf("skipping 5M scale test: expected at least 5,000,000 rows in orders_int (found %d; run generate_fixtures to populate full benchmark dataset)", totalRows)
	}

	t.Logf("Found %d rows in PostgreSQL. Starting Dynamic Work Rebalance Test...", totalRows)

	sqlReader, err := sql_adapter.NewSQLReader("postgres", db)
	if err != nil {
		t.Fatalf("failed creating sql reader: %v", err)
	}

	dbConfig := &domain.DatabaseSourceConfig{
		Driver:        "postgres",
		ConnectionURI: connURI,
		Query:         "SELECT id, customer_id, amount, status, created_at FROM orders_int WHERE id > 0",
		PartitionConfig: domain.PartitionConfig{
			PartitionColumn: "id",
			BatchSize:       250000, // 20 slices of 250k rows
		},
		Schema: domain.Schema{
			Fields: []domain.Field{
				{Name: "id", Type: domain.TypeInt64, Nullable: false},
				{Name: "customer_id", Type: domain.TypeString, Nullable: false},
				{Name: "amount", Type: domain.TypeFloat64, Nullable: false},
				{Name: "status", Type: domain.TypeString, Nullable: false},
				{Name: "created_at", Type: domain.TypeTimestamp, Nullable: false},
			},
		},
	}

	slices, err := sqlReader.CalculatePartitions(ctx, dbConfig)
	if err != nil {
		t.Fatalf("failed calculating partitions: %v", err)
	}
	t.Logf("Generated %d partition slices (batch size: 250,000)", len(slices))

	outputDir := filepath.Join(t.TempDir(), "e2e-scale-5m")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("failed creating output dir: %v", err)
	}
	outputPath := filepath.Join(outputDir, "orders.parquet")

	fOut, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("failed creating parquet output file: %v", err)
	}
	pqWriter := parquet.NewWriter(fOut, streams.BuildParquetSchema(&dbConfig.Schema))

	sinkChan := make(chan *domain.GenericRecord, 1024)
	var (
		writerWG  sync.WaitGroup
		writerErr error
		w1Count   int64
		w2Count   int64
	)

	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		defer fOut.Close()
		defer pqWriter.Close()
		for rec := range sinkChan {
			rowMap := streams.RecordToParquetRow(rec, &dbConfig.Schema)
			if err := pqWriter.Write(rowMap); err != nil {
				writerErr = err
				return
			}
		}
	}()

	startTime := time.Now()

	// 1. Worker 1 starts with the entire range [0, len(slices))
	initialRest := partitions.PartitionRange{Start: 0, End: len(slices)}
	w1Tracker := partitions.NewPartitionRangeTracker(initialRest)

	var (
		workerWG       sync.WaitGroup
		residualHolder atomic.Pointer[partitions.PartitionRange]
		splitTriggered atomic.Bool
	)

	// Worker 1 Goroutine
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		for {
			rest := w1Tracker.GetRestriction().(partitions.PartitionRange)
			currentProgress, _ := w1Tracker.GetProgress()
			sliceIdx := int(currentProgress) + rest.Start

			if sliceIdx >= rest.End {
				break
			}

			if !w1Tracker.TryClaim(sliceIdx) {
				break
			}

			recChan, errChan, err := sqlReader.ReadPartition(ctx, dbConfig, slices[sliceIdx])
			if err != nil {
				t.Errorf("worker 1 error reading slice %d: %v", sliceIdx, err)
				return
			}

			for rec := range recChan {
				atomic.AddInt64(&w1Count, 1)
				sinkChan <- rec
			}
			for err := range errChan {
				if err != nil {
					t.Errorf("worker 1 stream error in slice %d: %v", sliceIdx, err)
				}
			}

			// Trigger dynamic split when Worker 1 finishes slice 4 (after ~1.25M records)
			if sliceIdx == 4 && !splitTriggered.Swap(true) {
				t.Logf("Worker 1 completed slice 4. Triggering dynamic TrySplit(0.5)...")
				primary, residual, splitErr := w1Tracker.TrySplit(0.5)
				if splitErr != nil {
					t.Errorf("failed TrySplit: %v", splitErr)
				} else if residual != nil {
					pRange := primary.(partitions.PartitionRange)
					rRange := residual.(partitions.PartitionRange)
					t.Logf("Dynamic Split Successful: Primary Range: [%d, %d), Residual Range: [%d, %d)",
						pRange.Start, pRange.End, rRange.Start, rRange.End)
					residualHolder.Store(&rRange)
				}
			}
		}
	}()

	// Worker 2 (Dynamic Stealing / Rebalanced Worker)
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		// Wait until residual is available
		for residualHolder.Load() == nil {
			if w1Tracker.IsDone() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}

		resRange := *residualHolder.Load()
		t.Logf("Worker 2 activated! Stealing residual range [%d, %d)...", resRange.Start, resRange.End)
		w2Tracker := partitions.NewPartitionRangeTracker(resRange)

		for sliceIdx := resRange.Start; sliceIdx < resRange.End; sliceIdx++ {
			if !w2Tracker.TryClaim(sliceIdx) {
				break
			}

			recChan, errChan, err := sqlReader.ReadPartition(ctx, dbConfig, slices[sliceIdx])
			if err != nil {
				t.Errorf("worker 2 error reading slice %d: %v", sliceIdx, err)
				return
			}

			for rec := range recChan {
				atomic.AddInt64(&w2Count, 1)
				sinkChan <- rec
			}
			for err := range errChan {
				if err != nil {
					t.Errorf("worker 2 stream error in slice %d: %v", sliceIdx, err)
				}
			}
		}
	}()

	workerWG.Wait()
	close(sinkChan)
	writerWG.Wait()

	duration := time.Since(startTime)
	totalProcessed := atomic.LoadInt64(&w1Count) + atomic.LoadInt64(&w2Count)

	t.Logf("----------------------------------------------------------------------")
	t.Logf("SDF Scale & Dynamic Work Rebalance Test Results:")
	t.Logf("Total Time: %v", duration)
	t.Logf("Throughput: %.2f rows/sec", float64(totalProcessed)/duration.Seconds())
	t.Logf("Worker 1 Processed: %d rows", atomic.LoadInt64(&w1Count))
	t.Logf("Worker 2 Processed: %d rows (via dynamic work-stealing split)", atomic.LoadInt64(&w2Count))
	t.Logf("Total Processed:    %d rows", totalProcessed)
	t.Logf("----------------------------------------------------------------------")

	if writerErr != nil {
		t.Fatalf("writer failed with error: %v", writerErr)
	}

	if totalProcessed != totalRows {
		t.Errorf("records lost/duplicated! Expected %d, got %d", totalRows, totalProcessed)
	}

	if atomic.LoadInt64(&w2Count) == 0 {
		t.Errorf("Worker 2 did not process any records! Dynamic split failed to rebalance work.")
	}

	// Verify Parquet file integrity
	pf, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("failed opening output parquet file: %v", err)
	}
	defer pf.Close()

	fi, err := pf.Stat()
	if err != nil {
		t.Fatalf("failed stat parquet file: %v", err)
	}

	pqReader := parquet.NewGenericReader[parquetRowRecord](pf)
	defer pqReader.Close()

	if pqReader.NumRows() != totalRows {
		t.Errorf("parquet row count mismatch! Expected %d, got %d", totalRows, pqReader.NumRows())
	}

	t.Logf("Verified Parquet File: %d rows written cleanly, size: %.2f MB",
		pqReader.NumRows(), float64(fi.Size())/(1024*1024))
}
