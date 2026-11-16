package source

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"github.com/omnibeam/dataflow-compute-go/pkg/jsonutils"
)

var _ ports.PagedAPIReader = (*RESTReader)(nil)

// RESTReader implements ports.PagedAPIReader for REST HTTP APIs.
type RESTReader struct {
	client *HTTPClient
	cfg    domain.APISourceConfig
}

// NewRESTReader creates a new RESTReader adapter.
func NewRESTReader(cfg domain.APISourceConfig, secrets map[string]string) *RESTReader {
	return &RESTReader{
		client: NewHTTPClient(cfg, secrets),
		cfg:    cfg,
	}
}

// EstimatePages estimates or constructs the initial list of page slices for distributed SDF execution.
func (r *RESTReader) EstimatePages(ctx context.Context, apiCfg any) ([]ports.PageSlice, error) {
	cfg, ok := apiCfg.(domain.APISourceConfig)
	if !ok {
		cfg = r.cfg
	}

	switch cfg.Pagination.Type {
	case domain.PaginationPageNumber, domain.PaginationOffsetLimit:
		return r.estimateNumberedOrOffsetPages(ctx, cfg), nil
	default:
		// cursor_token and link_header start with 1 initial page slice
		return []ports.PageSlice{{
			PageIndex: 0,
			PageSize:  cfg.Pagination.PageSize,
		}}, nil
	}
}

func (r *RESTReader) estimateNumberedOrOffsetPages(ctx context.Context, cfg domain.APISourceConfig) []ports.PageSlice {
	initialPage := cfg.Pagination.InitialPage
	if initialPage == 0 && cfg.Pagination.Type == domain.PaginationPageNumber {
		initialPage = 1
	}

	startOffset := 0
	if cfg.Pagination.Type == domain.PaginationOffsetLimit && cfg.WatermarkIsOffset() && cfg.Watermark.Value != nil {
		if n, err := strconv.Atoi(strings.TrimSpace(*cfg.Watermark.Value)); err == nil && n > 0 {
			startOffset = n
		}
	}

	if cfg.Pagination.TotalPagesHint > 0 {
		return buildHintedSlices(cfg, initialPage, startOffset)
	}

	if cfg.Pagination.TotalCountPath != "" && cfg.Pagination.PageSize > 0 {
		if totalCount, ok := r.probeTotalCount(ctx, cfg, initialPage); ok && totalCount > 0 {
			return buildSlicesFromTotalCount(cfg, totalCount, initialPage, startOffset)
		}
	}

	defaultPage := initialPage
	if cfg.Pagination.Type == domain.PaginationOffsetLimit && startOffset > 0 {
		defaultPage = startOffset
	}
	return []ports.PageSlice{{
		PageIndex: defaultPage,
		PageSize:  cfg.Pagination.PageSize,
	}}
}

func buildHintedSlices(cfg domain.APISourceConfig, initialPage, startOffset int) []ports.PageSlice {
	slices := make([]ports.PageSlice, cfg.Pagination.TotalPagesHint)
	for i := 0; i < cfg.Pagination.TotalPagesHint; i++ {
		pageIdx := i + initialPage
		if cfg.Pagination.Type == domain.PaginationOffsetLimit {
			pageIdx = startOffset + (i * cfg.Pagination.PageSize)
		}
		slices[i] = ports.PageSlice{
			PageIndex: pageIdx,
			PageSize:  cfg.Pagination.PageSize,
		}
	}
	return slices
}

func (r *RESTReader) probeTotalCount(ctx context.Context, cfg domain.APISourceConfig, initialPage int) (float64, bool) {
	pageParam := cfg.Pagination.PageParam
	if pageParam == "" {
		pageParam = "page"
	}
	sizeParam := cfg.Pagination.SizeParam
	if sizeParam == "" {
		sizeParam = "size"
	}
	probeURL := fmt.Sprintf("%s%s?%s=%d&%s=1",
		cfg.BaseURL, cfg.Endpoint, pageParam, initialPage, sizeParam,
	)
	req, err := http.NewRequestWithContext(ctx, "GET", probeURL, nil)
	if err != nil {
		return 0, false
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	totalVal, err := jsonutils.ExtractPath(body, cfg.Pagination.TotalCountPath)
	if err != nil || totalVal == nil {
		return 0, false
	}

	switch v := totalVal.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case string:
		parsed, perr := strconv.ParseFloat(v, 64)
		if perr != nil {
			slog.Warn("ignoring unparseable total count from API probe", "value", v, "error", perr)
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func buildSlicesFromTotalCount(cfg domain.APISourceConfig, totalCount float64, initialPage, startOffset int) []ports.PageSlice {
	if cfg.Pagination.Type == domain.PaginationOffsetLimit && startOffset > 0 {
		if float64(startOffset) >= totalCount {
			return []ports.PageSlice{{
				PageIndex: startOffset,
				PageSize:  cfg.Pagination.PageSize,
			}}
		}
		remaining := totalCount - float64(startOffset)
		numPages := int(math.Ceil(remaining / float64(cfg.Pagination.PageSize)))
		slices := make([]ports.PageSlice, numPages)
		for i := 0; i < numPages; i++ {
			slices[i] = ports.PageSlice{
				PageIndex: startOffset + (i * cfg.Pagination.PageSize),
				PageSize:  cfg.Pagination.PageSize,
			}
		}
		return slices
	}

	numPages := int(math.Ceil(totalCount / float64(cfg.Pagination.PageSize)))
	slices := make([]ports.PageSlice, numPages)
	for i := 0; i < numPages; i++ {
		pageIdx := i + initialPage
		if cfg.Pagination.Type == domain.PaginationOffsetLimit {
			pageIdx = i * cfg.Pagination.PageSize
		}
		slices[i] = ports.PageSlice{
			PageIndex: pageIdx,
			PageSize:  cfg.Pagination.PageSize,
		}
	}
	return slices
}

// ReadPage fetches a single page and emits records on a channel.
func (r *RESTReader) ReadPage(
	ctx context.Context,
	apiCfg any,
	page ports.PageSlice,
) (<-chan *domain.GenericRecord, <-chan error, error) {
	cfg, ok := apiCfg.(domain.APISourceConfig)
	if !ok {
		cfg = r.cfg
	}

	recChan := make(chan *domain.GenericRecord, 100)
	errChan := make(chan error, 1)

	go func() {
		defer close(recChan)
		defer close(errChan)

		reqURL, err := r.buildRequestURL(cfg, page)
		if err != nil {
			errChan <- fmt.Errorf("failed to build request url: %w", err)
			return
		}

		httpMethod := cfg.HTTPMethod
		if httpMethod == "" {
			httpMethod = "GET"
		}

		req, err := http.NewRequestWithContext(ctx, httpMethod, reqURL, nil)
		if err != nil {
			errChan <- fmt.Errorf("failed to create http request: %w", err)
			return
		}

		resp, err := r.client.Do(req)
		if err != nil {
			errChan <- fmt.Errorf("http request error: %w", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			errChan <- fmt.Errorf("api returned status %d: %s", resp.StatusCode, string(body))
			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			errChan <- fmt.Errorf("failed to read response body: %w", err)
			return
		}

		records, err := jsonutils.ExtractRecords(body, cfg.RecordsPath, cfg.FieldMapping, cfg.Schema, "api_source")
		if err != nil {
			errChan <- fmt.Errorf("failed to extract records: %w", err)
			return
		}

		for _, rec := range records {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			case recChan <- rec:
			}
		}
	}()

	return recChan, errChan, nil
}

func (r *RESTReader) buildRequestURL(cfg domain.APISourceConfig, page ports.PageSlice) (string, error) {
	baseURL := cfg.BaseURL + cfg.Endpoint
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	q := parsedURL.Query()
	for k, v := range cfg.QueryParams {
		q.Set(k, v)
	}

	if cfg.Watermark != nil && cfg.Watermark.Value != nil && strings.TrimSpace(*cfg.Watermark.Value) != "" && !cfg.WatermarkIsOffset() {
		q.Set(cfg.Watermark.Column, strings.TrimSpace(*cfg.Watermark.Value))
	}

	switch cfg.Pagination.Type {
	case domain.PaginationPageNumber:
		pageParam := cfg.Pagination.PageParam
		if pageParam == "" {
			pageParam = "page"
		}
		sizeParam := cfg.Pagination.SizeParam
		if sizeParam == "" {
			sizeParam = "page_size"
		}
		q.Set(pageParam, strconv.Itoa(page.PageIndex))
		if page.PageSize > 0 {
			q.Set(sizeParam, strconv.Itoa(page.PageSize))
		}

	case domain.PaginationOffsetLimit:
		offsetParam := cfg.Pagination.PageParam
		if offsetParam == "" {
			offsetParam = "offset"
		}
		limitParam := cfg.Pagination.SizeParam
		if limitParam == "" {
			limitParam = "limit"
		}
		q.Set(offsetParam, strconv.Itoa(page.PageIndex))
		if page.PageSize > 0 {
			q.Set(limitParam, strconv.Itoa(page.PageSize))
		}

	case domain.PaginationCursorToken:
		if page.Cursor != "" {
			cursorParam := cfg.Pagination.CursorParam
			if cursorParam == "" {
				cursorParam = "cursor"
			}
			q.Set(cursorParam, page.Cursor)
		}
		if page.PageSize > 0 && cfg.Pagination.SizeParam != "" {
			q.Set(cfg.Pagination.SizeParam, strconv.Itoa(page.PageSize))
		}
	}

	parsedURL.RawQuery = q.Encode()
	return parsedURL.String(), nil
}
