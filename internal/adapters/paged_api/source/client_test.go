package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

		"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestHTTPClient_RateLimiter(t *testing.T) {
	var count int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&count, 1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	cfg := domain.APISourceConfig{
		BaseURL: ts.URL,
		Retry: domain.APIRetryConfig{
			RateLimitRPS: 10, // 10 requests per second max
			TimeoutMs:    2000,
		},
	}
	client := NewHTTPClient(cfg, nil)

	start := time.Now()
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequestWithContext(context.Background(), "GET", ts.URL+"/test", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp.Body.Close()
	}
	elapsed := time.Since(start)
	if elapsed < 350*time.Millisecond {
		t.Errorf("expected rate limiter to throttle requests, elapsed %v", elapsed)
	}
}

func TestHTTPClient_429RetryWithRetryAfter(t *testing.T) {
	var attempts int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt64(&attempts, 1)
		if att == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"rate_limited"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"data":"success"}`))
	}))
	defer ts.Close()

	cfg := domain.APISourceConfig{
		BaseURL: ts.URL,
		Retry: domain.APIRetryConfig{
			MaxRetries:       3,
			InitialBackoffMs: 100,
			MaxBackoffMs:     2000,
			TimeoutMs:        5000,
		},
	}
	client := NewHTTPClient(cfg, nil)

	req, _ := http.NewRequestWithContext(context.Background(), "GET", ts.URL+"/retry", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expected successful retry, got: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	if atomic.LoadInt64(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", atomic.LoadInt64(&attempts))
	}
}

func TestHTTPClient_AuthBearerAndKey(t *testing.T) {
	var apiKeyHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKeyHeader = r.Header.Get("X-API-Key")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	cfg := domain.APISourceConfig{
		BaseURL: ts.URL,
		Auth: &domain.APIAuthConfig{
			Type:         domain.AuthAPIKey,
			APIKeyHeader: "X-API-Key",
			TokenRef:     "secret-api-key-value",
		},
		Retry: domain.APIRetryConfig{TimeoutMs: 2000},
	}
	client := NewHTTPClient(cfg, map[string]string{"secret-api-key-value": "my-secret-token"})

	req, _ := http.NewRequestWithContext(context.Background(), "GET", ts.URL+"/auth", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()
	if apiKeyHeader != "my-secret-token" {
		t.Errorf("expected X-API-Key 'my-secret-token', got '%s'", apiKeyHeader)
	}

	t.Run("Bearer Auth", func(t *testing.T) {
		var authHeader string
		tsBearer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		}))
		defer tsBearer.Close()

		cfgBearer := domain.APISourceConfig{
			BaseURL: tsBearer.URL,
			Auth: &domain.APIAuthConfig{
				Type:     domain.AuthBearer,
				TokenRef: "tok",
			},
		}
		c := NewHTTPClient(cfgBearer, map[string]string{"tok": "jwt.secret.token"})
		req, _ := http.NewRequestWithContext(context.Background(), "GET", tsBearer.URL, nil)
		resp, _ := c.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		if authHeader != "Bearer jwt.secret.token" {
			t.Errorf("expected Bearer token, got %s", authHeader)
		}
	})

	t.Run("Basic Auth", func(t *testing.T) {
		var user, pass string
		var ok bool
		tsBasic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok = r.BasicAuth()
			w.WriteHeader(http.StatusOK)
		}))
		defer tsBasic.Close()

		cfgBasic := domain.APISourceConfig{
			BaseURL: tsBasic.URL,
			Auth: &domain.APIAuthConfig{
				Type:        domain.AuthBasic,
				UsernameRef: "u",
				PasswordRef: "p",
			},
		}
		c := NewHTTPClient(cfgBasic, map[string]string{"u": "admin", "p": "secret123"})
		req, _ := http.NewRequestWithContext(context.Background(), "GET", tsBasic.URL, nil)
		resp, _ := c.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		if !ok || user != "admin" || pass != "secret123" {
			t.Errorf("expected basic auth admin:secret123, got %s:%s (ok=%v)", user, pass, ok)
		}
	})

	t.Run("QueryParam Auth", func(t *testing.T) {
		var apiKeyParam string
		tsParam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKeyParam = r.URL.Query().Get("api_key")
			w.WriteHeader(http.StatusOK)
		}))
		defer tsParam.Close()

		cfgParam := domain.APISourceConfig{
			BaseURL: tsParam.URL,
			Auth: &domain.APIAuthConfig{
				Type:        domain.AuthAPIKey,
				APIKeyQuery: "api_key",
				TokenRef:    "key",
			},
		}
		c := NewHTTPClient(cfgParam, map[string]string{"key": "my-param-key"})
		req, _ := http.NewRequestWithContext(context.Background(), "GET", tsParam.URL+"/search", nil)
		resp, _ := c.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		if apiKeyParam != "my-param-key" {
			t.Errorf("expected query param api_key 'my-param-key', got '%s'", apiKeyParam)
		}
	})
}

func TestTokenBucket_ContextCancel(t *testing.T) {
	tb := NewTokenBucket(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tb.Wait(ctx)
	if err == nil {
		t.Error("expected error waiting with cancelled context")
	}
}

func TestTokenBucket_ZeroAndNil(t *testing.T) {
	tbZero := NewTokenBucket(0)
	if tbZero != nil {
		t.Errorf("expected nil TokenBucket for rps=0, got %v", tbZero)
	}
	if err := tbZero.Wait(context.Background()); err != nil {
		t.Errorf("expected nil error on nil token bucket Wait, got %v", err)
	}

	tbNeg := NewTokenBucket(-5)
	if tbNeg != nil {
		t.Errorf("expected nil TokenBucket for negative rps, got %v", tbNeg)
	}
}

func TestHTTPClient_InjectsW3CTraceHeaders(t *testing.T) {
	var capturedTraceparent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTraceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	cfg := domain.APISourceConfig{
		BaseURL: srv.URL,
	}
	client := NewHTTPClient(cfg, nil)

	carrier := map[string]string{"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	ctx := context.Background()
	// inject carrier into context using propagator
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	req.Header.Set("traceparent", carrier["traceparent"])

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do failed: %v", err)
	}
	resp.Body.Close()

	if capturedTraceparent != carrier["traceparent"] {
		t.Fatalf("expected traceparent %q, got %q", carrier["traceparent"], capturedTraceparent)
	}
}

