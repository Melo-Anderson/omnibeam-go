# Stage 1: Build static Go binary
FROM golang:alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /opt/dataflow/pipeline ./cmd/pipeline

# Stage 2: Dataflow Flex Template Launcher
FROM gcr.io/dataflow-templates-base/go-template-launcher-base:latest AS launcher

# Stage 3: Runtime
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 65532 pipeline
COPY --from=launcher /opt/google/dataflow/go_template_launcher /opt/google/dataflow/go_template_launcher
COPY --from=builder /opt/dataflow/pipeline /opt/dataflow/pipeline
ENV FLEX_TEMPLATE_GO_BINARY="/opt/dataflow/pipeline"
USER 65532:65532
ENTRYPOINT ["/opt/google/dataflow/go_template_launcher"]
