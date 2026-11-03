package sink

import (
	"context"
	"errors"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type stubSecrets struct {
	value string
	err   error
}

func (s stubSecrets) Resolve(context.Context, string) (string, error) { return s.value, s.err }

func credentialCfg(ref string) *domain.PipelineConfig {
	return &domain.PipelineConfig{
		Destination: domain.DestinationConfig{
			Endpoint: domain.APIEndpointConfig{CredentialRef: ref},
		},
	}
}

func TestRestWriterFactory_CredentialResolutionErrorIsPropagated(t *testing.T) {
	sentinel := errors.New("secret backend down")
	_, err := restWriterFactory(context.Background(), credentialCfg("gcp:token"), stubSecrets{err: sentinel})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel error, got %v", err)
	}
}

func TestRestSinkBuilderFactory_CredentialResolutionErrorIsPropagated(t *testing.T) {
	sentinel := errors.New("secret backend down")
	_, err := restSinkBuilderFactory(context.Background(), credentialCfg("gcp:token"), ports.SinkDeps{Secrets: stubSecrets{err: sentinel}})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel error, got %v", err)
	}
}

func TestRestWriterFactory_NoCredentialRefSkipsResolution(t *testing.T) {
	if _, err := restWriterFactory(context.Background(), credentialCfg(""), stubSecrets{err: errors.New("must not be called")}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
