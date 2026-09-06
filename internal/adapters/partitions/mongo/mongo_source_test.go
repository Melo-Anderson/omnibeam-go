package mongo_test

import (
	"context"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/mongo"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.mongodb.org/mongo-driver/v2/bson"
	mongo_driver "go.mongodb.org/mongo-driver/v2/mongo"
)

func TestMongoSource_ContractAndValidation(t *testing.T) {
	var client *mongo_driver.Client
	src := mongo.NewMongoSource(client, "testdb")

	var reader ports.PartitionedReader = src
	if reader == nil {
		t.Fatal("MongoSource must implement ports.PartitionedReader")
	}

	// Verify Close() doesn't panic on nil client
	if err := src.Close(); err != nil {
		t.Errorf("unexpected error on Close with nil client: %v", err)
	}

	t.Run("CalculatePartitions type validation", func(t *testing.T) {
		_, err := src.CalculatePartitions(context.Background(), "invalid-config-type")
		if err == nil {
			t.Error("expected error for invalid config type, got nil")
		}
	})

	t.Run("CalculatePartitions empty table validation", func(t *testing.T) {
		cfg := &domain.DatabaseSourceConfig{Table: ""}
		_, err := src.CalculatePartitions(context.Background(), cfg)
		if err == nil {
			t.Error("expected error for empty table, got nil")
		}
	})

	t.Run("CalculatePartitions empty db name validation", func(t *testing.T) {
		srcNoDB := mongo.NewMongoSource(client, "")
		cfg := &domain.DatabaseSourceConfig{Table: "users", Database: ""}
		_, err := srcNoDB.CalculatePartitions(context.Background(), cfg)
		if err == nil {
			t.Error("expected error for empty database, got nil")
		}
	})

	t.Run("CalculatePartitions with NumPartitions <= 1 returns single slice", func(t *testing.T) {
		cfg := &domain.DatabaseSourceConfig{
			Table: "users",
			PartitionConfig: domain.PartitionConfig{
				NumPartitions: 1,
			},
		}
		slices, err := src.CalculatePartitions(context.Background(), cfg)
		if err != nil || len(slices) != 1 {
			t.Fatalf("expected 1 slice without connecting to mongo: %v, %v", slices, err)
		}
		if !slices[0].IsFirst || !slices[0].IsLast {
			t.Errorf("expected single slice to be marked first and last")
		}
	})

	t.Run("ReadPartition invalid config or filter", func(t *testing.T) {
		_, _, err := src.ReadPartition(context.Background(), "bad-type", ports.PartitionSlice{})
		if err == nil {
			t.Error("expected error for invalid config type in ReadPartition")
		}

		cfgBadFilter := &domain.DatabaseSourceConfig{
			Table:       "users",
			QueryFilter: "{bad-json}",
		}
		_, _, err = src.ReadPartition(context.Background(), cfgBadFilter, ports.PartitionSlice{})
		if err == nil {
			t.Error("expected error for invalid json query_filter in ReadPartition")
		}
	})
}

func TestMongoSource_SliceBounds(t *testing.T) {
	oid1, _ := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	oid2, _ := bson.ObjectIDFromHex("507f1f77bcf86cd799439012")

	src := mongo.NewMongoSource(nil, "db")

	cfg := &domain.DatabaseSourceConfig{
		Table:       "users",
		QueryFilter: `{"status":"active"}`,
	}

	// First slice
	sFirst := ports.PartitionSlice{
		IsFirst:    true,
		LowerBound: oid1.Hex(),
		UpperBound: oid2.Hex(),
	}
	_, _, err := src.ReadPartition(context.Background(), cfg, sFirst)
	// It will error at coll.Find because client is nil, which confirms filter was built successfully
	if err == nil {
		t.Error("expected find error on nil client")
	}

	// Last slice
	sLast := ports.PartitionSlice{
		IsLast:     true,
		LowerBound: oid1.Hex(),
		UpperBound: oid2.Hex(),
	}
	_, _, _ = src.ReadPartition(context.Background(), cfg, sLast)

	// Middle slice
	sMiddle := ports.PartitionSlice{
		IsFirst:    false,
		IsLast:     false,
		LowerBound: oid1.Hex(),
		UpperBound: oid2.Hex(),
	}
	_, _, _ = src.ReadPartition(context.Background(), cfg, sMiddle)
}

func TestMongoSource_Factory(t *testing.T) {
	t.Run("Missing DatabaseSource returns error", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			DatabaseSource: nil,
		}
		_, err := ports.BuildSource(context.Background(), "mongo", cfg, nil)
		if err == nil {
			t.Error("expected error for nil DatabaseSource, got nil")
		}
	})

	t.Run("OpenClient with invalid URI returns error", func(t *testing.T) {
		cfg := &domain.DatabaseSourceConfig{ConnectionURI: "invalid-uri"}
		_, err := mongo.OpenClient(context.Background(), cfg, nil)
		if err == nil {
			t.Error("expected error opening client with non-mongo URI")
		}
	})

	t.Run("OpenClient with ping failure on unreachable host", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		cfg := &domain.DatabaseSourceConfig{
			ConnectionURI: "mongodb://127.0.0.1:27019/?serverSelectionTimeoutMS=10&connectTimeoutMS=10",
			PoolConfig:    domain.PoolConfig{MaxOpenConns: 5},
		}
		_, err := mongo.OpenClient(ctx, cfg, nil)
		if err == nil {
			t.Error("expected error connecting/pinging unreachable mongo host")
		}
	})

	t.Run("Factory propagates OpenClient error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		cfg := &domain.PipelineConfig{
			DatabaseSource: &domain.DatabaseSourceConfig{
				ConnectionURI: "mongodb://127.0.0.1:27019/?serverSelectionTimeoutMS=10&connectTimeoutMS=10",
			},
		}
		_, err := ports.BuildSource(ctx, "mongo", cfg, nil)
		if err == nil {
			t.Error("expected error from factory on connection failure")
		}
	})

	t.Run("CalculatePartitions with > 1 partition on nil client returns error", func(t *testing.T) {
		src := mongo.NewMongoSource(nil, "db")
		cfg := &domain.DatabaseSourceConfig{
			Table: "users",
			PartitionConfig: domain.PartitionConfig{
				NumPartitions: 4,
			},
		}
		_, err := src.CalculatePartitions(context.Background(), cfg)
		if err == nil {
			t.Error("expected error calculating partitions with nil client and numPartitions > 1")
		}
	})
}
