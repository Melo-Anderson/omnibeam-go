package domain

// ============================================================================
// OmniBeam Platform Defaults Catalog
// Centralized operational and performance tuning defaults across all connectors.
// ============================================================================

const (
	// DefaultPipelineRunner is the default execution runner for pipelines.
	DefaultPipelineRunner = "direct"

	// File & ByteStream Source Defaults
	DefaultSourceFormat      = "csv"
	DefaultSourceDelimiter   = ","
	DefaultSourceQuoteChar   = "\""
	DefaultSourceMultiline   = false
	DefaultSourceCharset     = "utf-8"
	DefaultSourceCompression = "none"
	// DefaultChunkSizeBytes is the default byte size for splitting non-compressed files in ByteStreamSourceSDF (64 MB).
	DefaultChunkSizeBytes = int64(64 * 1024 * 1024)
	// DefaultMinSplitChunkSizeBytes is the minimum sub-megabyte split resolution for SDF liquid sharding (64 KB).
	DefaultMinSplitChunkSizeBytes = int64(64 * 1024)

	// Database Source Defaults
	// DefaultDatabaseBatchSize is the default target row count per partition slice for relational and NoSQL databases.
	DefaultDatabaseBatchSize = 2500
	// DefaultPoolMaxOpenConns is the default maximum number of open connections in worker database connection pools.
	DefaultPoolMaxOpenConns = 4
	// DefaultPoolMaxIdleConns is the default maximum number of idle connections in worker database connection pools.
	DefaultPoolMaxIdleConns = 2
	// DefaultPoolConnLifetimeSeconds is the default maximum connection lifetime in seconds.
	DefaultPoolConnLifetimeSeconds = 300
	// DefaultDatabaseCircuitBreakerFails is the default consecutive failures before opening database circuit breaker.
	DefaultDatabaseCircuitBreakerFails = 5

	// Resilience & Fault Tolerance Defaults
	DefaultCircuitBreakerTimeoutMs     = 10000 // 10s
	DefaultCircuitBreakerHalfOpenLimit = 1
	DefaultDrainTimeoutMs              = 15000 // 15s
	DefaultBackoffMultiplier           = 2.0

	// REST API Source & HTTP Transport Defaults
	// DefaultAPIPageSize is the default record count per page for REST API endpoints when unspecified.
	DefaultAPIPageSize            = 100
	DefaultAPIMaxPagesLimit       = 10000
	DefaultAPIMaxRetries          = 3
	DefaultAPIInitialBackoffMs    = 500
	DefaultAPIMaxBackoffMs        = 10000
	DefaultAPITimeoutMs           = 30000
	DefaultAPICircuitBreakerFails = 5
	DefaultAPIRateLimitRPS        = 50.0
	DefaultUserAgent              = "OmniBeam/1.0"
	DefaultAcceptHeader           = "application/json"
	DefaultContentType            = "application/json"
	DefaultHTTPMaxIdleConns       = 100
	DefaultHTTPMaxIdleConnsPerHost = 20
	DefaultHTTPIdleConnTimeoutMs  = 90000

	// Destination & Sinks Defaults
	DefaultDestinationType         = "file"
	DefaultDestinationOutputFormat = "parquet"
	DefaultDestinationDelimiter      = ","
	DefaultDestinationLineTerminator = "\n"
	DefaultDestinationCharset        = "utf-8"
	DefaultDestinationAPIMethod      = "POST"
	DefaultDestinationAPIBatchSize   = 100
	DefaultBigQueryBatchSize         = 500
	DefaultAdaptiveMaxBatchFactor    = 10
	DefaultDestinationAPITimeoutMs   = 30000
	DefaultDestinationAPIRateLimit   = 50
	DefaultDestinationAPIMaxRetries  = 3

	// Serialization, Decoding & Coder Defaults
	// DefaultMaxRecordFields is the maximum allowed column count per GenericRecord in binary coders.
	DefaultMaxRecordFields = 10000
	// DefaultStackMaskBytes is the stack-allocated bitmask capacity for null masks in binary coders (16 bytes = 128 fields).
	DefaultStackMaskBytes = 16
	// DefaultMaskPoolBytes is the pooled buffer size for null masks in wide tables (64 bytes = 512 fields).
	DefaultMaskPoolBytes = 64

	// Stream & I/O Prefetching Defaults
	// DefaultPrefetchChunkSizeBytes is the default buffer chunk size for async prefetch reading (64 KiB).
	DefaultPrefetchChunkSizeBytes = 64 * 1024
	// DefaultPrefetchBufferCount is the default number of chunks queued in memory for async prefetchers.
	DefaultPrefetchBufferCount = 2

	// Columnar RecordBatch Defaults
	// DefaultRecordBatchCapacity is the default number of rows per columnar RecordBatch (5,000 rows).
	DefaultRecordBatchCapacity = 5000
	// DefaultMinRecordBatchCapacity is the minimum allowed row capacity for a RecordBatch.
	DefaultMinRecordBatchCapacity = 64
	// DefaultMaxRecordBatchCapacity is the maximum allowed row capacity for a RecordBatch.
	DefaultMaxRecordBatchCapacity = 50000

	// String Interning & Memory Bounds Defaults
	// DefaultMaxInternStringLen is the maximum string byte length eligible for memory interning.
	DefaultMaxInternStringLen = 64
	// DefaultMaxInternPoolSize is the maximum number of deduplicated entries stored in string intern pools.
	DefaultMaxInternPoolSize = 16384
	// DefaultInitialPoolCapacity is the initial allocation capacity for string intern pools.
	DefaultInitialPoolCapacity = 1024

	// Observability & Telemetry Context Keys
	TraceIDKey = "trace_id"
	SpanIDKey  = "span_id"
)

// OrDefault returns value if it is not the zero-value for type T; otherwise returns fallback.
func OrDefault[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}
