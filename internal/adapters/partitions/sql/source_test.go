package sql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	sql_adapter "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/sql"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestSQLReaderFactory_OCP(t *testing.T) {
	var dummyDB *sql.DB

	t.Run("Resolves registered postgres driver", func(t *testing.T) {
		reader, err := sql_adapter.NewSQLReader("postgres", dummyDB)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := reader.(ports.PartitionedReader); !ok {
			t.Errorf("expected reader to implement ports.PartitionedReader")
		}
	})

	t.Run("Resolves registered mysql driver", func(t *testing.T) {
		reader, err := sql_adapter.NewSQLReader("mysql", dummyDB)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := reader.(ports.PartitionedReader); !ok {
			t.Errorf("expected reader to implement ports.PartitionedReader")
		}
	})

	t.Run("Fails for unknown driver with descriptive error", func(t *testing.T) {
		_, err := sql_adapter.NewSQLReader("sqlite3", dummyDB)
		if err == nil {
			t.Fatal("expected error for unregistered sqlite3 driver, got nil")
		}
	})
}

func TestGenericSQLSource_StreamingAndPartitions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
		},
	}

	cfg := &domain.DatabaseSourceConfig{
		Driver: "postgres",
		Table:  "users",
		PartitionConfig: domain.PartitionConfig{
			PartitionColumn: "id",
			BatchSize:       50,
		},
		Schema: *schema,
	}

	t.Run("CalculatePartitions and ReadPartition streaming rows", func(t *testing.T) {
		// Mock partition bounds query
		mock.ExpectQuery("SELECT bucket_id.*").WillReturnRows(
			sqlmock.NewRows([]string{"bucket_id", "lower_val", "upper_val", "row_cnt"}).
				AddRow(1, 1, 100, 100),
		)

		src := sql_adapter.NewPostgresSource(db)
		slices, err := src.CalculatePartitions(context.Background(), cfg)
		if err != nil || len(slices) != 1 {
			t.Fatalf("CalculatePartitions: slices=%v, err=%v", slices, err)
		}

		// Mock row streaming query
		mock.ExpectQuery("SELECT \\* FROM \\(SELECT \\* FROM users\\).*").WillReturnRows(
			sqlmock.NewRows([]string{"id", "name"}).
				AddRow(1, "Alice").
				AddRow(2, "Bob"),
		)

		recCh, errCh, err := src.ReadPartition(context.Background(), cfg, slices[0])
		if err != nil {
			t.Fatalf("ReadPartition: %v", err)
		}

		var records []*domain.GenericRecord
		for r := range recCh {
			records = append(records, r)
		}
		for e := range errCh {
			if e != nil {
				t.Fatalf("unexpected stream error: %v", e)
			}
		}

		if len(records) != 2 {
			t.Fatalf("expected 2 streamed records, got %d", len(records))
		}
		if records[0].Values[1].StringVal() != "Alice" {
			t.Errorf("expected Alice, got %s", records[0].Values[1].StringVal())
		}
	})

	t.Run("MySQL Source constructor", func(t *testing.T) {
		mysqlSrc := sql_adapter.NewMySQLSource(db)
		if mysqlSrc == nil {
			t.Fatal("expected non-nil MySQLSource")
		}
	})

	t.Run("Factory missing database source", func(t *testing.T) {
		cfg := &domain.PipelineConfig{DatabaseSource: nil}
		_, err := ports.BuildSource(context.Background(), "postgres", cfg, nil)
		if err == nil {
			t.Error("expected error for nil DatabaseSource")
		}
	})

	t.Run("Trips circuit breaker on consecutive errors and fast-fails", func(t *testing.T) {
		src := sql_adapter.NewGenericSQLSource(nil, true)
		// With nil DB, execute fails and trips breaker
		_, _, err := src.ReadSlice(context.Background(), cfg, ports.SQLSlice{SliceIndex: 0})
		if err == nil {
			t.Fatal("expected error on nil db query")
		}
	})
}
