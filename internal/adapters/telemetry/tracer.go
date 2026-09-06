// Package telemetry configures OpenTelemetry trace export for the pipeline.
// SRP: this package is the only place that knows about the concrete OTLP exporter.
// DIP: callers in cmd/pipeline use this via the returned func — no import in beam/ or domain/.
package telemetry

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InitTracer initializes an OTLP gRPC TracerProvider when an endpoint is explicitly configured.
// If no endpoint is configured via OTEL_EXPORTER_OTLP_ENDPOINT (or if tracing is disabled),
// it returns a no-op shutdown function and does not attempt any network connections.
//
// Dynamic configuration via standard OTel environment variables:
//  - OTEL_EXPORTER_OTLP_ENDPOINT: target gRPC endpoint (e.g. "localhost:4317" or "jaeger:4317"). If empty, tracing is no-op.
//  - OTEL_SDK_DISABLED: if "true", disables OTel tracing completely and returns a no-op shutdown.
//  - OTEL_TRACES_EXPORTER: if "none", disables trace export and returns a no-op shutdown.
//  - OTEL_SERVICE_NAME: overrides the default service name.
func InitTracer(ctx context.Context, defaultServiceName string) (shutdown func(context.Context) error, err error) {
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") || strings.EqualFold(os.Getenv("OTEL_TRACES_EXPORTER"), "none") {
		return func(context.Context) error { return nil }, nil
	}

	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		// Purely opt-in: no hardcoded localhost, IPs, or fallbacks in code.
		return func(context.Context) error { return nil }, nil
	}

	serviceName := defaultServiceName
	if envSvc := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); envSvc != "" {
		serviceName = envSvc
	}

	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}
