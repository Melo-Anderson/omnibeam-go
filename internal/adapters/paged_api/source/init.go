package source

import (
	"context"
	"fmt"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	ports.RegisterPagedAPISource("rest_api", restReaderFactory)
	ports.RegisterPagedAPISource("http_api", restReaderFactory)
}

func restReaderFactory(ctx context.Context, cfg *domain.PipelineConfig, sec ports.SecretResolver) (ports.PagedAPIReader, error) {
	if cfg.APISource == nil {
		return nil, fmt.Errorf("api_source configuration is missing")
	}
	resolvedSecrets := make(map[string]string)
	if cfg.APISource.Auth != nil && cfg.APISource.Auth.TokenRef != "" && sec != nil {
		secret, err := sec.Resolve(ctx, cfg.APISource.Auth.TokenRef)
		if err == nil {
			resolvedSecrets[cfg.APISource.Auth.TokenRef] = secret
		}
	}
	return NewRESTReader(*cfg.APISource, resolvedSecrets), nil
}
