package source

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/pkg/resilience"
	"github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
)

// HTTPClient wraps standard http.Client with rate limiting, retries, auth injection, and circuit breaker.
type HTTPClient struct {
	httpClient *http.Client
	cfg        domain.APISourceConfig
	limiter    *TokenBucket
	secrets    map[string]string
	cb         *resilience.CircuitBreaker
}

// NewHTTPClient instantiates a resilient HTTPClient.
func NewHTTPClient(cfg domain.APISourceConfig, secrets map[string]string) *HTTPClient {
	cfg.ApplyDefaults()

	timeout := time.Duration(cfg.Retry.TimeoutMs) * time.Millisecond
	transport := &http.Transport{
		MaxIdleConns:        domain.DefaultHTTPMaxIdleConns,
		MaxIdleConnsPerHost: domain.DefaultHTTPMaxIdleConnsPerHost,
		IdleConnTimeout:     time.Duration(domain.DefaultHTTPIdleConnTimeoutMs) * time.Millisecond,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.SkipTLSVerify,
		},
	}

	cbTimeout := time.Duration(cfg.Retry.CircuitBreakerTimeoutMs) * time.Millisecond
	cb, _ := resilience.NewCircuitBreaker(resilience.Settings{
		Name:        "http-client-cb",
		MaxFailures: uint32(cfg.Retry.CircuitBreakerFails),
		Timeout:     cbTimeout,
	})

	return &HTTPClient{
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		cfg:     cfg,
		limiter: NewTokenBucket(cfg.Retry.RateLimitRPS),
		secrets: secrets,
		cb:      cb,
	}
}

func isTransientStatus(code int) bool {
	return code == http.StatusTooManyRequests || (code >= 500 && code <= 599)
}

func parseRetryAfter(header string) time.Duration {
	if s, err := strconv.Atoi(header); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return 0
}

func (c *HTTPClient) resolveSecret(ref string) string {
	if ref == "" {
		return ""
	}
	if val, ok := c.secrets[ref]; ok {
		return val
	}
	return ref
}

func (c *HTTPClient) injectAuth(req *http.Request) {
	if c.cfg.Auth == nil {
		return
	}
	switch c.cfg.Auth.Type {
	case domain.AuthBearer:
		if token := c.resolveSecret(c.cfg.Auth.TokenRef); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	case domain.AuthAPIKey:
		keyVal := c.resolveSecret(c.cfg.Auth.TokenRef)
		if c.cfg.Auth.APIKeyHeader != "" {
			req.Header.Set(c.cfg.Auth.APIKeyHeader, keyVal)
		} else if c.cfg.Auth.APIKeyQuery != "" {
			q := req.URL.Query()
			q.Set(c.cfg.Auth.APIKeyQuery, keyVal)
			req.URL.RawQuery = q.Encode()
		}
	case domain.AuthBasic:
		user := c.resolveSecret(c.cfg.Auth.UsernameRef)
		pass := c.resolveSecret(c.cfg.Auth.PasswordRef)
		auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
		req.Header.Set("Authorization", "Basic "+auth)
	}
}

func (c *HTTPClient) prepareRequest(req *http.Request) {
	carrier := make(map[string]string)
	telemetry.InjectTraceContext(req.Context(), carrier)
	for k, v := range carrier {
		if req.Header.Get(k) == "" {
			req.Header.Set(k, v)
		}
	}

	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", domain.DefaultUserAgent)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", domain.DefaultAcceptHeader)
	}
	for k, v := range c.cfg.Headers {
		req.Header.Set(k, v)
	}

	c.injectAuth(req)
}

func (c *HTTPClient) buildBackoffConfig() resilience.BackoffConfig {
	return resilience.BackoffConfig{
		Initial:    time.Duration(c.cfg.Retry.InitialBackoffMs) * time.Millisecond,
		Max:        time.Duration(c.cfg.Retry.MaxBackoffMs) * time.Millisecond,
		Multiplier: c.cfg.Retry.BackoffMultiplier,
		MaxRetries: c.cfg.Retry.MaxRetries,
	}
}

func (c *HTTPClient) sendWithCircuitBreaker(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	execFn := func() error {
		var err error
		resp, err = c.httpClient.Do(req)
		if err != nil {
			return err
		}
		if isTransientStatus(resp.StatusCode) {
			return fmt.Errorf("transient http status: %d", resp.StatusCode)
		}
		return nil
	}

	if c.cb != nil {
		err := c.cb.Execute(execFn)
		return resp, err
	}
	return resp, execFn()
}

func (c *HTTPClient) calculateSleep(attempt int, resp *http.Response, cfg resilience.BackoffConfig) time.Duration {
	if resp != nil {
		if delay := parseRetryAfter(resp.Header.Get("Retry-After")); delay > 0 {
			return delay
		}
	}
	return resilience.FullJitter(attempt, cfg)
}

// Do executes an HTTP request with rate limiting, circuit breaker, jitter, and trace propagation.
func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	if c.cb != nil && c.cb.State() == resilience.StateOpen {
		return nil, resilience.ErrCircuitOpen
	}

	c.prepareRequest(req)
	backoffCfg := c.buildBackoffConfig()

	var lastErr error
	for attempt := 0; attempt <= c.cfg.Retry.MaxRetries; attempt++ {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter wait cancelled: %w", err)
		}

		resp, opErr := c.sendWithCircuitBreaker(req)
		if opErr == nil && resp != nil && !isTransientStatus(resp.StatusCode) {
			return resp, nil
		}

		lastErr = opErr
		if resp != nil {
			resp.Body.Close()
		}

		if attempt < c.cfg.Retry.MaxRetries {
			sleepDuration := c.calculateSleep(attempt, resp, backoffCfg)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleepDuration):
			}
		}
	}

	return nil, fmt.Errorf("request failed after %d retries: %w", c.cfg.Retry.MaxRetries, lastErr)
}
