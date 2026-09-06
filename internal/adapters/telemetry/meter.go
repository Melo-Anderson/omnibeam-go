// Package telemetry configures OpenTelemetry trace and metric export for the pipeline.
// SRP: this package is the only place that knows about the concrete OTLP exporter.
// DIP: callers in cmd/pipeline use this via the returned func — no import in beam/ or domain/.
package telemetry

import (
	"context"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// InitMeter initializes an OTLP gRPC MeterProvider when an endpoint is explicitly configured.
// If no endpoint is configured via OTEL_EXPORTER_OTLP_ENDPOINT (or if metrics are disabled),
// it returns a no-op shutdown function and does not attempt any network connections.
//
// Dynamic configuration via standard OTel environment variables:
//   - OTEL_EXPORTER_OTLP_ENDPOINT: target gRPC endpoint (e.g. "localhost:4317" or "jaeger:4317"). If empty, metrics are no-op.
//   - OTEL_SDK_DISABLED: if "true", disables OTel metrics completely and returns a no-op shutdown.
//   - OTEL_METRICS_EXPORTER: if "none", disables metric export and returns a no-op shutdown.
//   - OTEL_SERVICE_NAME: overrides the default service name.
//   - WORKER_ID: sets the service.instance.id for worker disaggregation.
func InitMeter(ctx context.Context, defaultServiceName string) (shutdown func(context.Context) error, err error) {
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") || strings.EqualFold(os.Getenv("OTEL_METRICS_EXPORTER"), "none") {
		return func(context.Context) error { return nil }, nil
	}

	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	serviceName := defaultServiceName
	if envSvc := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); envSvc != "" {
		serviceName = envSvc
	}

	instanceID := strings.TrimSpace(os.Getenv("WORKER_ID"))
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}

	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceInstanceIDKey.String(instanceID),
		),
	)
	if err != nil {
		return nil, err
	}

	reader := sdkmetric.NewPeriodicReader(
		exporter,
		sdkmetric.WithInterval(5*time.Second),
	)

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return mp.Shutdown, nil
}
