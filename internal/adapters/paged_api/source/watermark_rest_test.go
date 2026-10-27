package source_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/paged_api/source"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestEstimatePages_OffsetWatermarkShiftsTotalPagesHint(t *testing.T) {
	val := "1500"
	cfg := domain.APISourceConfig{
		BaseURL:  "https://api.empresa.com",
		Endpoint: "/v1/users",
		Pagination: domain.APIPaginationConfig{
			Type:           domain.PaginationOffsetLimit,
			PageSize:       100,
			TotalPagesHint: 3,
		},
		Watermark: &domain.WatermarkConfig{
			Column: "offset",
			Type:   domain.WatermarkTypeInteger,
			Value:  &val,
		},
	}

	reader := source.NewRESTReader(cfg, nil)
	slices, err := reader.EstimatePages(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed estimating pages: %v", err)
	}
	if len(slices) != 3 {
		t.Fatalf("expected 3 slices, got %d", len(slices))
	}
	expectedIndices := []int{1500, 1600, 1700}
	for i, s := range slices {
		if s.PageIndex != expectedIndices[i] {
			t.Errorf("slice[%d] PageIndex want %d, got %d", i, expectedIndices[i], s.PageIndex)
		}
	}
}

func TestEstimatePages_OffsetWatermarkSubtractsStartFromTotalCount(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": 1750,
		})
	}))
	defer ts.Close()

	val := "1500"
	cfg := domain.APISourceConfig{
		BaseURL:  ts.URL,
		Endpoint: "/users",
		Pagination: domain.APIPaginationConfig{
			Type:           domain.PaginationOffsetLimit,
			PageSize:       100,
			TotalCountPath: "total",
		},
		Watermark: &domain.WatermarkConfig{
			Column: "offset",
			Type:   domain.WatermarkTypeInteger,
			Value:  &val,
		},
	}

	reader := source.NewRESTReader(cfg, nil)
	slices, err := reader.EstimatePages(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed estimating pages: %v", err)
	}
	// 1750 - 1500 = 250 remaining -> ceil(250/100) = 3 pages (offsets: 1500, 1600, 1700)
	if len(slices) != 3 {
		t.Fatalf("expected 3 slices, got %d", len(slices))
	}
	expected := []int{1500, 1600, 1700}
	for i, s := range slices {
		if s.PageIndex != expected[i] {
			t.Errorf("slice[%d] PageIndex want %d, got %d", i, expected[i], s.PageIndex)
		}
	}
}

func TestEstimatePages_OffsetWatermarkAtOrBeyondTotalYieldsSingleSliceAtStart(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total": 1000,
		})
	}))
	defer ts.Close()

	val := "1500"
	cfg := domain.APISourceConfig{
		BaseURL:  ts.URL,
		Endpoint: "/users",
		Pagination: domain.APIPaginationConfig{
			Type:           domain.PaginationOffsetLimit,
			PageSize:       100,
			TotalCountPath: "total",
		},
		Watermark: &domain.WatermarkConfig{
			Column: "offset",
			Type:   domain.WatermarkTypeInteger,
			Value:  &val,
		},
	}

	reader := source.NewRESTReader(cfg, nil)
	slices, err := reader.EstimatePages(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed estimating pages: %v", err)
	}
	if len(slices) != 1 || slices[0].PageIndex != 1500 {
		t.Fatalf("expected 1 slice with PageIndex 1500, got: %+v", slices)
	}
}

func TestReadPage_NonOffsetWatermarkSendsQueryParam(t *testing.T) {
	var receivedParam string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedParam = r.URL.Query().Get("updated_since")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer ts.Close()

	tsVal := "2026-10-24T00:00:00+00:00"
	cfg := domain.APISourceConfig{
		BaseURL:  ts.URL,
		Endpoint: "/events",
		Pagination: domain.APIPaginationConfig{
			Type:     domain.PaginationPageNumber,
			PageSize: 50,
		},
		Watermark: &domain.WatermarkConfig{
			Column: "updated_since",
			Type:   domain.WatermarkTypeTimestamp,
			Value:  &tsVal,
		},
	}

	reader := source.NewRESTReader(cfg, nil)
	recChan, errChan, err := reader.ReadPage(context.Background(), cfg, ports.PageSlice{PageIndex: 1, PageSize: 50})
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	for range recChan {
	}
	for err := range errChan {
		if err != nil {
			t.Fatalf("unexpected error from stream: %v", err)
		}
	}

	if receivedParam != tsVal {
		t.Fatalf("expected query param updated_since=%q, got %q", tsVal, receivedParam)
	}
}

func TestReadPage_NoWatermarkValueSendsNoExtraParam(t *testing.T) {
	var receivedParam string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedParam = r.URL.Query().Get("updated_since")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer ts.Close()

	cfg := domain.APISourceConfig{
		BaseURL:  ts.URL,
		Endpoint: "/events",
		Pagination: domain.APIPaginationConfig{
			Type:     domain.PaginationPageNumber,
			PageSize: 50,
		},
		Watermark: &domain.WatermarkConfig{
			Column: "updated_since",
			Type:   domain.WatermarkTypeTimestamp,
			Value:  nil,
		},
	}

	reader := source.NewRESTReader(cfg, nil)
	recChan, errChan, err := reader.ReadPage(context.Background(), cfg, ports.PageSlice{PageIndex: 1, PageSize: 50})
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	for range recChan {
	}
	for err := range errChan {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if receivedParam != "" {
		t.Fatalf("expected empty param for initial run, got %q", receivedParam)
	}
}
