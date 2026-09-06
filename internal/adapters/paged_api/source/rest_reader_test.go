package source

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestRESTReader_PageNumberPagination(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		w.Header().Set("Content-Type", "application/json")
		if page == "1" {
			w.Write([]byte(`{"total": 4, "items": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}]}`))
		} else if page == "2" {
			w.Write([]byte(`{"total": 4, "items": [{"id": 3, "name": "Item 3"}, {"id": 4, "name": "Item 4"}]}`))
		} else {
			w.Write([]byte(`{"total": 4, "items": []}`))
		}
	}))
	defer ts.Close()

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
	}

	cfg := domain.APISourceConfig{
		BaseURL:     ts.URL,
		Endpoint:    "/items",
		RecordsPath: "items",
		Pagination: domain.APIPaginationConfig{
			Type:           domain.PaginationPageNumber,
			PageParam:      "page",
			SizeParam:      "size",
			PageSize:       2,
			TotalCountPath: "total",
		},
		Schema: schema,
	}

	reader := NewRESTReader(cfg, nil)
	ctx := context.Background()

	slices, err := reader.EstimatePages(ctx, cfg)
	if err != nil {
		t.Fatalf("EstimatePages failed: %v", err)
	}
	if len(slices) != 2 {
		t.Fatalf("expected 2 page slices, got %d", len(slices))
	}

	recChan, errChan, err := reader.ReadPage(ctx, cfg, slices[0])
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	var count int
	for rec := range recChan {
		count++
		if rec == nil {
			t.Error("received nil record")
		}
	}
	for err := range errChan {
		t.Fatalf("unexpected error channel emission: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 records in page 1, got %d", count)
	}
}

func TestRESTReader_OffsetLimitPagination(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset := r.URL.Query().Get("offset")
		w.Header().Set("Content-Type", "application/json")
		if offset == "0" {
			w.Write([]byte(`{"total_count": 4, "data": [{"id": 1}, {"id": 2}]}`))
		} else if offset == "2" {
			w.Write([]byte(`{"total_count": 4, "data": [{"id": 3}, {"id": 4}]}`))
		} else {
			w.Write([]byte(`{"total_count": 4, "data": []}`))
		}
	}))
	defer ts.Close()

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: "int64"},
		},
	}

	cfg := domain.APISourceConfig{
		BaseURL:     ts.URL,
		Endpoint:    "/data",
		RecordsPath: "data",
		Pagination: domain.APIPaginationConfig{
			Type:           domain.PaginationOffsetLimit,
			PageParam:      "offset",
			SizeParam:      "limit",
			PageSize:       2,
			TotalCountPath: "total_count",
		},
		Schema: schema,
	}

	reader := NewRESTReader(cfg, nil)
	ctx := context.Background()

	slices, err := reader.EstimatePages(ctx, cfg)
	if err != nil {
		t.Fatalf("EstimatePages failed: %v", err)
	}
	if len(slices) != 2 {
		t.Fatalf("expected 2 page slices, got %d", len(slices))
	}
	if slices[0].PageIndex != 0 || slices[1].PageIndex != 2 {
		t.Errorf("expected offsets 0 and 2, got %d and %d", slices[0].PageIndex, slices[1].PageIndex)
	}

	recChan, errChan, err := reader.ReadPage(ctx, cfg, slices[1])
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	var count int
	for range recChan {
		count++
	}
	for err := range errChan {
		t.Fatalf("unexpected error: %v", err)
	}

	if count != 2 {
		t.Errorf("expected 2 records, got %d", count)
	}
}

func TestRESTReader_LinkHeaderPagination(t *testing.T) {
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" || page == "1" {
			w.Header().Set("Link", fmt.Sprintf(`<%s/items?page=2>; rel="next"`, ts.URL))
			w.Write([]byte(`[{"id": 1}, {"id": 2}]`))
		} else {
			w.Write([]byte(`[{"id": 3}]`))
		}
	}))
	defer ts.Close()

	cfg := domain.APISourceConfig{
		BaseURL:  ts.URL,
		Endpoint: "/items",
		Pagination: domain.APIPaginationConfig{
			Type:     domain.PaginationLinkHeader,
			PageSize: 2,
		},
		Schema: domain.Schema{Fields: []domain.Field{{Name: "id", Type: "int64"}}},
	}

	reader := NewRESTReader(cfg, nil)
	ctx := context.Background()

	slices, err := reader.EstimatePages(ctx, cfg)
	if err != nil {
		t.Fatalf("EstimatePages failed: %v", err)
	}
	if len(slices) != 1 {
		t.Fatalf("expected 1 initial slice for link_header, got %d", len(slices))
	}

	recChan, errChan, err := reader.ReadPage(ctx, cfg, slices[0])
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	var count int
	for range recChan {
		count++
	}
	for err := range errChan {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 records, got %d", count)
	}
}

func TestRESTReader_CursorAndFixedPages(t *testing.T) {
	ctx := context.Background()

	t.Run("Cursor Token pagination", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"next_cursor":"cur_abc123","items":[{"id":1}]}`))
		}))
		defer ts.Close()

		cfg := domain.APISourceConfig{
			BaseURL:     ts.URL,
			Endpoint:    "/cursor",
			RecordsPath: "items",
			Pagination: domain.APIPaginationConfig{
				Type:           domain.PaginationCursorToken,
				CursorParam:    "cursor",
				NextCursorPath: "next_cursor",
			},
			Schema: domain.Schema{Fields: []domain.Field{{Name: "id", Type: "int64"}}},
		}

		reader := NewRESTReader(cfg, nil)
		slices, err := reader.EstimatePages(ctx, cfg)
		if err != nil || len(slices) != 1 {
			t.Fatalf("EstimatePages cursor: %v, %v", slices, err)
		}

		recCh, _, err := reader.ReadPage(ctx, cfg, slices[0])
		if err != nil {
			t.Fatalf("ReadPage: %v", err)
		}
		var cnt int
		for range recCh {
			cnt++
		}
		if cnt != 1 {
			t.Errorf("expected 1 record, got %d", cnt)
		}
	})

	t.Run("Fixed pages estimation", func(t *testing.T) {
		cfg := domain.APISourceConfig{
			BaseURL:  "http://example.com",
			Endpoint: "/data",
			Pagination: domain.APIPaginationConfig{
				Type:           domain.PaginationPageNumber,
				TotalPagesHint: 5,
				PageParam:      "page",
			},
		}
		reader := NewRESTReader(cfg, nil)
		slices, err := reader.EstimatePages(ctx, cfg)
		if err != nil || len(slices) != 5 {
			t.Fatalf("expected 5 slices from fixed TotalPages, got %d (err: %v)", len(slices), err)
		}
	})

	t.Run("Fallback to default config on non-APISourceConfig", func(t *testing.T) {
		cfg := domain.APISourceConfig{
			BaseURL:  "http://example.com",
			Endpoint: "/data",
			Pagination: domain.APIPaginationConfig{
				Type:           domain.PaginationPageNumber,
				TotalPagesHint: 2,
				PageParam:      "page",
			},
		}
		reader := NewRESTReader(cfg, nil)
		slices, err := reader.EstimatePages(ctx, "different-type")
		if err != nil || len(slices) != 2 {
			t.Errorf("expected 2 slices using fallback config, got %d, err: %v", len(slices), err)
		}
	})

	t.Run("Ports BuildPagedAPISource factory", func(t *testing.T) {
		_, err := ports.BuildPagedAPISource(ctx, "rest_api", &domain.PipelineConfig{}, nil)
		if err == nil {
			t.Error("expected error for missing api_source config")
		}

		validCfg := &domain.PipelineConfig{
			APISource: &domain.APISourceConfig{
				BaseURL:  "http://example.com",
				Endpoint: "/test",
			},
		}
		reader, err := ports.BuildPagedAPISource(ctx, "rest_api", validCfg, nil)
		if err != nil || reader == nil {
			t.Errorf("expected successful factory build, got %v", err)
		}
	})

	t.Run("ReadPage error scenarios", func(t *testing.T) {
		errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/500") {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("internal error"))
				return
			}
			if strings.Contains(r.URL.Path, "/bad-json") {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("{malformed-json"))
				return
			}
			if strings.Contains(r.URL.Path, "/missing-path") {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"other_key": [1,2,3]}`))
				return
			}
		}))
		defer errServer.Close()

		schema := domain.Schema{
			Fields: []domain.Field{{Name: "id", Type: "int64"}},
		}

		// 500 error
		r500 := NewRESTReader(domain.APISourceConfig{
			BaseURL:  errServer.URL,
			Endpoint: "/500",
			Schema:   schema,
		}, nil)
		_, errCh500, _ := r500.ReadPage(ctx, nil, ports.PageSlice{PageIndex: 1})
		if err := <-errCh500; err == nil {
			t.Error("expected error for 500 status code in errChan")
		}

		// Bad JSON error
		rBadJSON := NewRESTReader(domain.APISourceConfig{
			BaseURL:     errServer.URL,
			Endpoint:    "/bad-json",
			RecordsPath: "data",
			Schema:      schema,
		}, nil)
		_, errChBad, _ := rBadJSON.ReadPage(ctx, nil, ports.PageSlice{PageIndex: 1})
		if err := <-errChBad; err == nil {
			t.Error("expected error for malformed json in errChan")
		}

		// Missing path emits 0 records (empty page)
		rMissingPath := NewRESTReader(domain.APISourceConfig{
			BaseURL:     errServer.URL,
			Endpoint:    "/missing-path",
			RecordsPath: "non_existent_data",
			Schema:      schema,
		}, nil)
		recCh, _, _ := rMissingPath.ReadPage(ctx, nil, ports.PageSlice{PageIndex: 1})
		var cnt int
		for range recCh {
			cnt++
		}
		if cnt != 0 {
			t.Errorf("expected 0 records for missing records path, got %d", cnt)
		}
	})
}
