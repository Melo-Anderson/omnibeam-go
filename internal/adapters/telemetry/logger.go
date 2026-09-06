package telemetry

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	pkgtelemetry "github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
)

// StructuredLogger emits structured JSON logs with OTel trace correlation.
type StructuredLogger struct {
	logger *slog.Logger
}

// NewStructuredLogger creates a StructuredLogger writing JSON to out.
// If out is nil, it defaults to os.Stdout.
func NewStructuredLogger(out io.Writer) *StructuredLogger {
	if out == nil {
		out = os.Stdout
	}
	handler := slog.NewJSONHandler(out, nil)
	return &StructuredLogger{logger: slog.New(handler)}
}

func withTraceFields(ctx context.Context, args []any) []any {
	traceID, spanID := pkgtelemetry.GetTraceAndSpanIDs(ctx)
	return append(args, slog.String(domain.TraceIDKey, traceID), slog.String(domain.SpanIDKey, spanID))
}

// sanitizeArgs redacts values for any key that matches sensitive keywords.
// It processes args in slog's key-value alternating format: [key, value, key, value, ...].
func sanitizeArgs(args []any, customSensitive []string) []any {
	if len(args) == 0 {
		return args
	}
	sanitized := make([]any, len(args))
	copy(sanitized, args)
	for i := 0; i+1 < len(args); i += 2 {
		if k, ok := args[i].(string); ok {
			sanitized[i+1] = pkgtelemetry.SanitizeValue(k, args[i+1], customSensitive)
		}
	}
	return sanitized
}

// InfoContext emits a structured INFO log with trace_id and span_id fields, sanitizing sensitive args.
func (l *StructuredLogger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.logger.InfoContext(ctx, msg, sanitizeArgs(withTraceFields(ctx, args), nil)...)
}

// ErrorContext emits a structured ERROR log with trace_id and span_id fields, sanitizing sensitive args.
func (l *StructuredLogger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.logger.ErrorContext(ctx, msg, sanitizeArgs(withTraceFields(ctx, args), nil)...)
}
