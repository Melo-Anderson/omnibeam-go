package domain_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestOrDefault(t *testing.T) {
	t.Run("Returns value when non-zero", func(t *testing.T) {
		if got := domain.OrDefault(42, 100); got != 42 {
			t.Errorf("expected 42, got %d", got)
		}
		if got := domain.OrDefault("custom", "fallback"); got != "custom" {
			t.Errorf("expected 'custom', got '%s'", got)
		}
		if got := domain.OrDefault(int64(1024), int64(2048)); got != int64(1024) {
			t.Errorf("expected 1024, got %d", got)
		}
	})

	t.Run("Returns fallback when zero-value", func(t *testing.T) {
		if got := domain.OrDefault(0, 100); got != 100 {
			t.Errorf("expected 100, got %d", got)
		}
		if got := domain.OrDefault("", "fallback"); got != "fallback" {
			t.Errorf("expected 'fallback', got '%s'", got)
		}
		if got := domain.OrDefault(int64(0), int64(2048)); got != int64(2048) {
			t.Errorf("expected 2048, got %d", got)
		}
	})
}

func TestStruct_ApplyDefaults(t *testing.T) {
	t.Run("PartitionConfig.ApplyDefaults populates DefaultDatabaseBatchSize", func(t *testing.T) {
		p := domain.PartitionConfig{}
		p.ApplyDefaults()
		if p.BatchSize != domain.DefaultDatabaseBatchSize {
			t.Errorf("expected BatchSize %d, got %d", domain.DefaultDatabaseBatchSize, p.BatchSize)
		}
	})

	t.Run("PoolConfig.ApplyDefaults populates pool defaults", func(t *testing.T) {
		p := domain.PoolConfig{}
		p.ApplyDefaults()
		if p.MaxOpenConns != domain.DefaultPoolMaxOpenConns {
			t.Errorf("expected MaxOpenConns %d, got %d", domain.DefaultPoolMaxOpenConns, p.MaxOpenConns)
		}
		if p.MaxIdleConns != domain.DefaultPoolMaxIdleConns {
			t.Errorf("expected MaxIdleConns %d, got %d", domain.DefaultPoolMaxIdleConns, p.MaxIdleConns)
		}
	})

	t.Run("APIRetryConfig.ApplyDefaults populates retry defaults", func(t *testing.T) {
		r := domain.APIRetryConfig{}
		r.ApplyDefaults()
		if r.MaxRetries != domain.DefaultAPIMaxRetries {
			t.Errorf("expected MaxRetries %d, got %d", domain.DefaultAPIMaxRetries, r.MaxRetries)
		}
		if r.TimeoutMs != domain.DefaultAPITimeoutMs {
			t.Errorf("expected TimeoutMs %d, got %d", domain.DefaultAPITimeoutMs, r.TimeoutMs)
		}
		if r.CircuitBreakerFails != domain.DefaultAPICircuitBreakerFails {
			t.Errorf("expected CircuitBreakerFails %d, got %d", domain.DefaultAPICircuitBreakerFails, r.CircuitBreakerFails)
		}
		if r.RateLimitRPS != domain.DefaultAPIRateLimitRPS {
			t.Errorf("expected RateLimitRPS %v, got %v", domain.DefaultAPIRateLimitRPS, r.RateLimitRPS)
		}
	})

	t.Run("APISourceConfig.ApplyDefaults populates defaults", func(t *testing.T) {
		cfg := domain.APISourceConfig{}
		cfg.ApplyDefaults()
		if cfg.HTTPMethod != "GET" {
			t.Errorf("expected HTTPMethod 'GET', got %s", cfg.HTTPMethod)
		}
		if cfg.Pagination.PageSize != domain.DefaultAPIPageSize {
			t.Errorf("expected PageSize %d, got %d", domain.DefaultAPIPageSize, cfg.Pagination.PageSize)
		}
	})
}

func TestAuditRemediationDefaults(t *testing.T) {
	if domain.DefaultMaxRecordFields <= 0 {
		t.Fatalf("expected positive DefaultMaxRecordFields, got %d", domain.DefaultMaxRecordFields)
	}
	if domain.DefaultPrefetchChunkSizeBytes <= 0 {
		t.Fatalf("expected positive DefaultPrefetchChunkSizeBytes, got %d", domain.DefaultPrefetchChunkSizeBytes)
	}
	if domain.DefaultPrefetchBufferCount <= 0 {
		t.Fatalf("expected positive DefaultPrefetchBufferCount, got %d", domain.DefaultPrefetchBufferCount)
	}
	if domain.DefaultMaxInternStringLen <= 0 {
		t.Fatalf("expected positive DefaultMaxInternStringLen, got %d", domain.DefaultMaxInternStringLen)
	}
	if domain.DefaultMaxInternPoolSize <= 0 {
		t.Fatalf("expected positive DefaultMaxInternPoolSize, got %d", domain.DefaultMaxInternPoolSize)
	}
	if domain.DefaultMinSplitChunkSizeBytes <= 0 {
		t.Fatalf("expected positive DefaultMinSplitChunkSizeBytes, got %d", domain.DefaultMinSplitChunkSizeBytes)
	}
	if domain.DefaultStackMaskBytes <= 0 {
		t.Fatalf("expected positive DefaultStackMaskBytes, got %d", domain.DefaultStackMaskBytes)
	}
	if domain.DefaultMaskPoolBytes <= 0 {
		t.Fatalf("expected positive DefaultMaskPoolBytes, got %d", domain.DefaultMaskPoolBytes)
	}
	if domain.DefaultRecordBatchCapacity <= 0 {
		t.Fatalf("expected positive DefaultRecordBatchCapacity, got %d", domain.DefaultRecordBatchCapacity)
	}
	if domain.DefaultMinRecordBatchCapacity <= 0 || domain.DefaultMinRecordBatchCapacity > domain.DefaultRecordBatchCapacity {
		t.Fatalf("invalid DefaultMinRecordBatchCapacity: %d", domain.DefaultMinRecordBatchCapacity)
	}
	if domain.DefaultMaxRecordBatchCapacity < domain.DefaultRecordBatchCapacity {
		t.Fatalf("invalid DefaultMaxRecordBatchCapacity: %d", domain.DefaultMaxRecordBatchCapacity)
	}
}
