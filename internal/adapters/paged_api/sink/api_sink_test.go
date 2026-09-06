package sink

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestAPISink_WriteBatch(t *testing.T) {
	receivedRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRequests++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	}))
	defer server.Close()

	endpoint := domain.APIEndpointConfig{
		BaseURL: server.URL,
	}
	options := domain.APISinkOptions{
		ResourcePath: "/v1/leads",
		Method:       "POST",
		BatchSize:    2,
		RateLimitRPS: 50,
	}

	apiSink := NewAPISink(endpoint, options, "")

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
	}

	r1 := domain.NewGenericRecord("src", 2)
	r1.SetInt64(0, 1)
	r1.SetString(1, "Lead 1")

	r2 := domain.NewGenericRecord("src", 2)
	r2.SetInt64(0, 2)
	r2.SetString(1, "Lead 2")

	succeeded, failed, err := apiSink.WriteBatch(context.Background(), []*domain.GenericRecord{r1, r2}, &schema)
	if err != nil {
		t.Fatalf("unexpected error writing batch: %v", err)
	}
	if len(succeeded) != 2 {
		t.Errorf("expected 2 succeeded, got %d", len(succeeded))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestAPISink_WriteBatch_HTTP400DLQ(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "invalid payload"}`))
	}))
	defer server.Close()

	endpoint := domain.APIEndpointConfig{
		BaseURL: server.URL,
	}
	options := domain.APISinkOptions{
		ResourcePath: "/v1/leads",
		Method:       "POST",
		BatchSize:    1,
		RateLimitRPS: 50,
	}

	apiSink := NewAPISink(endpoint, options, "")

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: "int64"},
		},
	}

	r1 := domain.NewGenericRecord("src", 1)
	r1.SetInt64(0, 999)

	succeeded, failed, err := apiSink.WriteBatch(context.Background(), []*domain.GenericRecord{r1}, &schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(succeeded) != 0 {
		t.Errorf("expected 0 succeeded, got %d", len(succeeded))
	}
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed record in DLQ, got %d", len(failed))
	}
	if failed[0].ErrorMessage == "" {
		t.Errorf("expected DLQ error message to be populated")
	}

	t.Run("Empty batch returns empty slices without error", func(t *testing.T) {
		s, f, err := apiSink.WriteBatch(context.Background(), nil, &schema)
		if err != nil || len(s) != 0 || len(f) != 0 {
			t.Errorf("expected empty results for nil records: %v, %v, %v", s, f, err)
		}
	})

	t.Run("HTTP 500 returns error after retry", func(t *testing.T) {
		server500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"internal_server_error"}`))
		}))
		defer server500.Close()

		sink500 := NewAPISink(domain.APIEndpointConfig{BaseURL: server500.URL}, domain.APISinkOptions{MaxRetries: 1}, "")
		s, f, err := sink500.WriteBatch(context.Background(), []*domain.GenericRecord{r1}, &schema)
		if err != nil || len(s) != 0 || len(f) != 1 {
			t.Errorf("expected 1 DLQ record on 500: s=%d, f=%d, err=%v", len(s), len(f), err)
		}
	})

	t.Run("Ports BuildBatchAPIWriter factory", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Destination: domain.DestinationConfig{
				Endpoint:   domain.APIEndpointConfig{},
				APIOptions: domain.APISinkOptions{},
			},
		}
		_, err := ports.BuildBatchAPIWriter(context.Background(), "rest_api", cfg, nil)
		if err != nil {
			t.Errorf("unexpected error from BuildBatchAPIWriter: %v", err)
		}
	})
}
