package domain

type PaginationType string

const (
	PaginationPageNumber  PaginationType = "page_number"
	PaginationOffsetLimit PaginationType = "offset_limit"
	PaginationCursorToken PaginationType = "cursor_token"
	PaginationLinkHeader  PaginationType = "link_header"
)

type AuthType string

const (
	AuthBearer           AuthType = "bearer_token"
	AuthAPIKey           AuthType = "api_key"
	AuthBasic            AuthType = "basic_auth"
	AuthOAuth2ClientCred AuthType = "oauth2_client_credentials"
)

type APIAuthConfig struct {
	Type            AuthType `json:"type"`
	TokenRef        string   `json:"token_ref,omitempty"`
	APIKeyHeader    string   `json:"api_key_header,omitempty"`
	APIKeyQuery     string   `json:"api_key_query,omitempty"`
	UsernameRef     string   `json:"username_ref,omitempty"`
	PasswordRef     string   `json:"password_ref,omitempty"`
	TokenURL        string   `json:"token_url,omitempty"`
	ClientIDRef     string   `json:"client_id_ref,omitempty"`
	ClientSecretRef string   `json:"client_secret_ref,omitempty"`
	Scopes          []string `json:"scopes,omitempty"`
}

type APIPaginationConfig struct {
	Type           PaginationType `json:"type"`
	PageParam      string         `json:"page_param,omitempty"`
	SizeParam      string         `json:"size_param,omitempty"`
	PageSize       int            `json:"page_size"`
	InitialPage    int            `json:"initial_page,omitempty"`
	TotalPagesHint int            `json:"total_pages_hint,omitempty"`
	MaxPagesLimit  int            `json:"max_pages_limit,omitempty"`
	CursorParam    string         `json:"cursor_param,omitempty"`
	NextCursorPath string         `json:"next_cursor_path,omitempty"`
	TotalCountPath string         `json:"total_count_path,omitempty"`
	HasMorePath    string         `json:"has_more_path,omitempty"`
}

// ApplyDefaults populates default values for omitted fields in APIPaginationConfig.
func (p *APIPaginationConfig) ApplyDefaults() {
	p.PageSize = OrDefault(p.PageSize, DefaultAPIPageSize)
	p.MaxPagesLimit = OrDefault(p.MaxPagesLimit, DefaultAPIMaxPagesLimit)
	if p.PageParam == "" {
		if p.Type == PaginationOffsetLimit {
			p.PageParam = "offset"
		} else {
			p.PageParam = "page"
		}
	}
	if p.SizeParam == "" {
		if p.Type == PaginationOffsetLimit {
			p.SizeParam = "limit"
		} else {
			p.SizeParam = "size"
		}
	}
	if p.InitialPage == 0 && p.Type == PaginationPageNumber {
		p.InitialPage = 1
	}
}

// ResilienceConfig defines comprehensive fault tolerance, backoff, and circuit breaker settings.
type ResilienceConfig struct {
	MaxRetries              int     `json:"max_retries,omitempty"`
	InitialBackoffMs        int     `json:"initial_backoff_ms,omitempty"`
	MaxBackoffMs            int     `json:"max_backoff_ms,omitempty"`
	BackoffMultiplier       float64 `json:"backoff_multiplier,omitempty"`
	RateLimitRPS            float64 `json:"rate_limit_rps,omitempty"`
	TimeoutMs               int     `json:"timeout_ms,omitempty"`
	CircuitBreakerFails     int     `json:"circuit_breaker_fails,omitempty"`
	CircuitBreakerTimeoutMs int     `json:"circuit_breaker_timeout_ms,omitempty"`
	HalfOpenLimit           int     `json:"half_open_limit,omitempty"`
}

// APIRetryConfig is a domain alias for ResilienceConfig for backward compatibility.
type APIRetryConfig = ResilienceConfig

// ApplyDefaults populates default values for omitted fields in ResilienceConfig.
func (r *ResilienceConfig) ApplyDefaults() {
	if r == nil {
		return
	}
	r.MaxRetries = OrDefault(r.MaxRetries, DefaultAPIMaxRetries)
	r.InitialBackoffMs = OrDefault(r.InitialBackoffMs, DefaultAPIInitialBackoffMs)
	r.MaxBackoffMs = OrDefault(r.MaxBackoffMs, DefaultAPIMaxBackoffMs)
	r.BackoffMultiplier = OrDefault(r.BackoffMultiplier, DefaultBackoffMultiplier)
	r.TimeoutMs = OrDefault(r.TimeoutMs, DefaultAPITimeoutMs)
	r.CircuitBreakerFails = OrDefault(r.CircuitBreakerFails, DefaultAPICircuitBreakerFails)
	r.CircuitBreakerTimeoutMs = OrDefault(r.CircuitBreakerTimeoutMs, DefaultCircuitBreakerTimeoutMs)
	r.RateLimitRPS = OrDefault(r.RateLimitRPS, DefaultAPIRateLimitRPS)
	r.HalfOpenLimit = OrDefault(r.HalfOpenLimit, DefaultCircuitBreakerHalfOpenLimit)
}

// Validate validates ResilienceConfig fields.
func (r *ResilienceConfig) Validate() error {
	if r == nil {
		return nil
	}
	return ValidateAll(
		CheckNonNegative("max_retries", r.MaxRetries),
		CheckNonNegative("initial_backoff_ms", r.InitialBackoffMs),
		CheckNonNegative("max_backoff_ms", r.MaxBackoffMs),
		CheckNonNegative("backoff_multiplier", r.BackoffMultiplier),
		CheckNonNegative("rate_limit_rps", r.RateLimitRPS),
		CheckNonNegative("timeout_ms", r.TimeoutMs),
		CheckNonNegative("circuit_breaker_fails", r.CircuitBreakerFails),
		CheckNonNegative("circuit_breaker_timeout_ms", r.CircuitBreakerTimeoutMs),
		CheckNonNegative("half_open_limit", r.HalfOpenLimit),
	)
}

type APISourceConfig struct {
	BaseURL       string              `json:"base_url"`
	Endpoint      string              `json:"endpoint"`
	HTTPMethod    string              `json:"http_method,omitempty"`
	Headers       map[string]string   `json:"headers,omitempty"`
	QueryParams   map[string]string   `json:"query_params,omitempty"`
	Auth          *APIAuthConfig      `json:"auth,omitempty"`
	Pagination    APIPaginationConfig `json:"pagination"`
	Retry         APIRetryConfig      `json:"retry,omitempty"`
	SkipTLSVerify bool                `json:"skip_tls_verify,omitempty"`
	RecordsPath   string              `json:"records_path,omitempty"`
	FieldMapping  map[string]string   `json:"field_mapping,omitempty"`
	Schema        Schema              `json:"schema"`
}

// ApplyDefaults populates default values for omitted fields in APISourceConfig.
func (cfg *APISourceConfig) ApplyDefaults() {
	if cfg == nil {
		return
	}
	cfg.HTTPMethod = OrDefault(cfg.HTTPMethod, "GET")
	cfg.Pagination.ApplyDefaults()
	cfg.Retry.ApplyDefaults()
}

type APIEndpointConfig struct {
	BaseURL       string            `json:"base_url"`
	CredentialRef string            `json:"credential_ref,omitempty"`
	AuthType      AuthType          `json:"auth_type,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
}

type APISinkOptions struct {
	ResourcePath string `json:"resource_path"`
	Method       string `json:"method,omitempty"`        // "POST", "PUT", "PATCH"
	BatchSize    int    `json:"batch_size,omitempty"`    // e.g. 200
	BodyEnvelope string `json:"body_envelope,omitempty"` // e.g. "records"
	RateLimitRPS int    `json:"rate_limit_rps,omitempty"`
	TimeoutMs    int    `json:"timeout_ms,omitempty"`
	MaxRetries   int    `json:"max_retries,omitempty"`
}

// ApplyDefaults populates default values for omitted fields in APISinkOptions.
func (opts *APISinkOptions) ApplyDefaults() {
	opts.Method = OrDefault(opts.Method, DefaultDestinationAPIMethod)
	opts.BatchSize = OrDefault(opts.BatchSize, DefaultDestinationAPIBatchSize)
	opts.RateLimitRPS = OrDefault(opts.RateLimitRPS, DefaultDestinationAPIRateLimit)
	opts.TimeoutMs = OrDefault(opts.TimeoutMs, DefaultDestinationAPITimeoutMs)
	opts.MaxRetries = OrDefault(opts.MaxRetries, DefaultDestinationAPIMaxRetries)
}
