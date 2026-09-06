package sink

import (
	"context"

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

func restWriterFactory(ctx context.Context, cfg *domain.PipelineConfig, sec ports.SecretResolver) (ports.BatchAPIWriter, error) {
	var token string
	if cfg.Destination.Endpoint.CredentialRef != "" && sec != nil {
		token, _ = sec.Resolve(ctx, cfg.Destination.Endpoint.CredentialRef)
	}
	return NewAPISink(cfg.Destination.Endpoint, cfg.Destination.APIOptions, token), nil
}

func restSinkBuilderFactory(ctx context.Context, cfg *domain.PipelineConfig, sec ports.SecretResolver, st ports.StorageBackend) (ports.BeamSinkBuilder, error) {
	writer, err := restWriterFactory(ctx, cfg, sec)
	if err != nil {
		return nil, err
	}
	var secretToken string
	if cfg.Destination.Endpoint.CredentialRef != "" && sec != nil {
		secretToken, _ = sec.Resolve(ctx, cfg.Destination.Endpoint.CredentialRef)
	}
	return &paged_api.BatchAPIBeamSink{
		Endpoint:    cfg.Destination.Endpoint,
		APIOptions:  cfg.Destination.APIOptions,
		SecretToken: secretToken,
		Writer:      writer,
		Schema:      cfg.GetSchema(),
		DLQPath:     cfg.DLQConfig.QuarantinePath,
		DLQSink:     st,
	}, nil
}
