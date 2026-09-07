package sql_test

import (
	"context"
	"testing"

	sql_adapter "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/sql"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestOpenDB_ConnectionURI(t *testing.T) {
	resolver := secrets.NewCompositeSecretResolver(nil, nil, "openbao")
	ctx := context.Background()

	t.Run("Fails when connection_uri is empty", func(t *testing.T) {
		cfg := &domain.DatabaseSourceConfig{
			Driver:        "postgres",
			ConnectionURI: "",
		}
		_, err := sql_adapter.OpenDB(ctx, cfg, resolver)
		if err == nil {
			t.Fatal("expected error for empty connection_uri, got nil")
		}
	})

	t.Run("Opens PostgreSQL DB with ConnectionURI and configures pool", func(t *testing.T) {
		cfg := &domain.DatabaseSourceConfig{
			Driver:        "postgres",
			ConnectionURI: "postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable",
			PoolConfig: domain.PoolConfig{
				MaxOpenConns:    5,
				MaxIdleConns:    2,
				ConnMaxLifetime: 120,
			},
		}

		db, err := sql_adapter.OpenDB(ctx, cfg, resolver)
		if err != nil {
			t.Fatalf("unexpected error building db connection: %v", err)
		}
		defer db.Close()

		stats := db.Stats()
		if stats.MaxOpenConnections != 5 {
			t.Errorf("expected max open conns 5, got %d", stats.MaxOpenConnections)
		}
	})

	t.Run("GetOrCreateDB reuses existing pool for identical driver and URI", func(t *testing.T) {
		sql_adapter.CloseCachedPoolsForTesting()
		defer sql_adapter.CloseCachedPoolsForTesting()

		cfg := &domain.DatabaseSourceConfig{
			Driver:        "postgres",
			ConnectionURI: "postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable",
		}

		db1, err := sql_adapter.GetOrCreateDB(ctx, cfg, resolver)
		if err != nil {
			t.Fatalf("first GetOrCreateDB failed: %v", err)
		}
		db2, err := sql_adapter.GetOrCreateDB(ctx, cfg, resolver)
		if err != nil {
			t.Fatalf("second GetOrCreateDB failed: %v", err)
		}

		if db1 != db2 {
			t.Errorf("expected identical *sql.DB pointer from pool cache, got %p and %p", db1, db2)
		}
	})

	t.Run("Opens DB with default pool config when empty", func(t *testing.T) {
		sql_adapter.CloseCachedPoolsForTesting()
		defer sql_adapter.CloseCachedPoolsForTesting()

		cfg := &domain.DatabaseSourceConfig{
			Driver:        "postgres",
			ConnectionURI: "postgres://testuser:testpass@localhost:5432/testdb2?sslmode=disable",
		}

		db, err := sql_adapter.OpenDB(ctx, cfg, resolver)
		if err != nil {
			t.Fatalf("OpenDB failed: %v", err)
		}
		stats := db.Stats()
		if stats.MaxOpenConnections != domain.DefaultPoolMaxOpenConns {
			t.Errorf("expected default MaxOpenConnections %d, got %d", domain.DefaultPoolMaxOpenConns, stats.MaxOpenConnections)
		}
	})
}
