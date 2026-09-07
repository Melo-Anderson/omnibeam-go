// Package sql implements relational database ingestion adapters and range slicing.
package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var (
	poolMu    sync.Mutex
	poolCache = make(map[string]*sql.DB)
)

// CloseDB removes the database connection from poolCache and closes it.
func CloseDB(db *sql.DB) error {
	if db == nil {
		return nil
	}
	poolMu.Lock()
	defer poolMu.Unlock()
	for k, v := range poolCache {
		if v == db {
			delete(poolCache, k)
			break
		}
	}
	return db.Close()
}

// CloseCachedPoolsForTesting clears and closes all pooled connections (used in test suites).
func CloseCachedPoolsForTesting() {
	poolMu.Lock()
	defer poolMu.Unlock()
	for k, db := range poolCache {
		_ = db.Close()
		delete(poolCache, k)
	}
}

var driverMap = map[string]string{
	"postgres": "pgx",
	"pgx":      "pgx",
	"mysql":    "mysql",
}

// GetOrCreateDB returns an existing cached connection pool or instantiates and caches a new one.
func GetOrCreateDB(ctx context.Context, cfg *domain.DatabaseSourceConfig, resolver ports.SecretResolver) (*sql.DB, error) {
	driverKey := strings.ToLower(strings.TrimSpace(cfg.Driver))
	driverName, ok := driverMap[driverKey]
	if !ok {
		driverName = driverKey // Allows any future driver registered via sql.Open
	}

	uri, err := secrets.ResolveConnectionURI(ctx, cfg.ConnectionURI, cfg.PasswordRef, resolver)
	if err != nil {
		return nil, fmt.Errorf("database connection uri resolution failed: %w", err)
	}

	cacheKey := fmt.Sprintf("%s:%s", driverName, uri)

	poolMu.Lock()
	defer poolMu.Unlock()

	if db, exists := poolCache[cacheKey]; exists {
		return db, nil
	}

	db, err := sql.Open(driverName, uri)
	if err != nil {
		return nil, fmt.Errorf("failed opening sql connection pool: %w", err)
	}

	configureConnectionPool(db, cfg.PoolConfig)

	poolCache[cacheKey] = db
	return db, nil
}

// OpenDB establishes and configures a database connection pool using ConnectionURI.
func OpenDB(ctx context.Context, cfg *domain.DatabaseSourceConfig, resolver ports.SecretResolver) (*sql.DB, error) {
	return GetOrCreateDB(ctx, cfg, resolver)
}

func configureConnectionPool(db *sql.DB, poolCfg domain.PoolConfig) {
	poolCfg.ApplyDefaults()
	db.SetMaxOpenConns(poolCfg.MaxOpenConns)
	db.SetMaxIdleConns(poolCfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(poolCfg.ConnMaxLifetime) * time.Second)
}
