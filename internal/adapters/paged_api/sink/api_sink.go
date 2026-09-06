package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	source_http "github.com/omnibeam/dataflow-compute-go/internal/adapters/paged_api/source"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var _ ports.BatchAPIWriter = (*APISink)(nil)

type APISink struct {
	endpoint   domain.APIEndpointConfig
	options    domain.APISinkOptions
	httpClient *source_http.HTTPClient
}

func NewAPISink(
	endpoint domain.APIEndpointConfig,
	options domain.APISinkOptions,
	secretToken string,
) *APISink {
	resolvedSecrets := make(map[string]string)
	if endpoint.CredentialRef != "" && secretToken != "" {
		resolvedSecrets[endpoint.CredentialRef] = secretToken
	}

	sourceCfg := domain.APISourceConfig{
		BaseURL: endpoint.BaseURL,
		Auth: &domain.APIAuthConfig{
			Type:     endpoint.AuthType,
			TokenRef: endpoint.CredentialRef,
		},
		Retry: domain.APIRetryConfig{
			MaxRetries:   options.MaxRetries,
			RateLimitRPS: float64(options.RateLimitRPS),
			TimeoutMs:    options.TimeoutMs,
		},
	}

	client := source_http.NewHTTPClient(sourceCfg, resolvedSecrets)

	return &APISink{
		endpoint:   endpoint,
		options:    options,
		httpClient: client,
	}
}

func (s *APISink) WriteBatch(
	ctx context.Context,
	records []*domain.GenericRecord,
	schema *domain.Schema,
) (succeeded []*domain.GenericRecord, failed []*domain.DeadLetterRecord, err error) {
	if schema == nil {
		return nil, nil, errors.New("schema is required")
	}
	if len(records) == 0 {
		return nil, nil, nil
	}

	items := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		if rec == nil {
			continue
		}
		items = append(items, rec.ToMap(schema))
	}

	var payload any = items
	if s.options.BodyEnvelope != "" {
		payload = map[string]any{
			s.options.BodyEnvelope: items,
		}
	} else if len(items) == 1 && s.options.BatchSize == 1 {
		payload = items[0]
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("failed marshaling batch payload: %w", err)
	}

	targetURL := s.endpoint.BaseURL + s.options.ResourcePath
	req, err := http.NewRequestWithContext(ctx, s.options.Method, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", domain.DefaultContentType)
	for k, v := range s.endpoint.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		// Network/retry failure: route entire batch to DLQ
		for range records {
			failed = append(failed, &domain.DeadLetterRecord{
				SourceFile:   targetURL,
				RawPayload:   string(bodyBytes),
				ErrorMessage: fmt.Sprintf("HTTP %s failed: %v", s.options.Method, err),
				FailedAt:     time.Now().UTC(),
			})
		}
		return nil, failed, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		errMsg := fmt.Sprintf("API returned HTTP status %d %s: %s", resp.StatusCode, resp.Status, string(respBody))
		for range records {
			failed = append(failed, &domain.DeadLetterRecord{
				SourceFile:   targetURL,
				RawPayload:   string(bodyBytes),
				ErrorMessage: errMsg,
				FailedAt:     time.Now().UTC(),
			})
		}
		return nil, failed, nil
	}

	return records, nil, nil
}
