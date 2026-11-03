package sink

import (
	"context"
	"fmt"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/paged_api"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	ports.RegisterBatchAPIWriter("rest_api", restWriterFactory)
	ports.RegisterBatchAPIWriter("http_api", restWriterFactory)

	ports.RegisterSink("rest_api", restSinkBuilderFactory)
	ports.RegisterSink("http_api", restSinkBuilderFactory)
}

func resolveCredential(ctx context.Context, sec ports.SecretResolver, ref string) (string, error) {
	if ref == "" || sec == nil {
		return "", nil
	}
	token, err := sec.Resolve(ctx, ref)
	if err != nil {
		return "", fmt.Errorf("resolving destination credential %q: %w", ref, err)
	}
	return token, nil
}

func restWriterFactory(ctx context.Context, cfg *domain.PipelineConfig, sec ports.SecretResolver) (ports.BatchAPIWriter, error) {
	token, err := resolveCredential(ctx, sec, cfg.Destination.Endpoint.CredentialRef)
	if err != nil {
		return nil, err
	}
	return NewAPISink(cfg.Destination.Endpoint, cfg.Destination.APIOptions, token), nil
}

func restSinkBuilderFactory(ctx context.Context, cfg *domain.PipelineConfig, deps ports.SinkDeps) (ports.BeamSinkBuilder, error) {
	writer, err := restWriterFactory(ctx, cfg, deps.Secrets)
	if err != nil {
		return nil, err
	}
	secretToken, err := resolveCredential(ctx, deps.Secrets, cfg.Destination.Endpoint.CredentialRef)
	if err != nil {
		return nil, err
	}
	return &paged_api.BatchAPIBeamSink{
		Endpoint:    cfg.Destination.Endpoint,
		APIOptions:  cfg.Destination.APIOptions,
		SecretToken: secretToken,
		Writer:      writer,
		Schema:      cfg.GetSchema(),
		DLQPath:     cfg.DLQConfig.QuarantinePath,
		DLQSink:     deps.Storage, // sourced from SinkDeps (ISP compliant)
	}, nil
}
