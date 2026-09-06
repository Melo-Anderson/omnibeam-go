// Package sql implements relational database ingestion adapters and range slicing.
package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var supportedDrivers = map[string]bool{
	"postgres":  true,
	"pgx":       true,
	"cockroach": true,
	"mysql":     true,
	"mariadb":   true,
}

// isDollarStyle returns true for PostgreSQL and compatible dialects that use $1, $2 parameter placeholders.
// All other standard relational databases (MySQL, SQLite, SQL Server, Oracle, ClickHouse) use ?.
func isDollarStyle(driver string) bool {
	d := strings.ToLower(strings.TrimSpace(driver))
	return d == "postgres" || d == "pgx" || d == "cockroach"
}

// NewSQLReader instantiates a universal SQLReader adapter configured for the target database engine.
func NewSQLReader(driver string, db *sql.DB) (ports.SQLReader, error) {
	d := strings.ToLower(strings.TrimSpace(driver))
	if !supportedDrivers[d] {
		return nil, fmt.Errorf("unsupported database driver: %q", driver)
	}
	return NewGenericSQLSource(db, isDollarStyle(d)), nil
}
