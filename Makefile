.PHONY: test lint build generate-fixtures test-functional docker-build docker-test-functional flex-template-build flex-template-run clean mutation-test

IMAGE_NAME ?= omnibeam-pipeline
VERSION ?= latest

lint:
	golangci-lint run ./...

test:
	go test -v -race -count=1 -cover ./internal/... ./pkg/...

mutation-test:
	gremlins unleash ./internal/domain/... ./internal/beam/core/...

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/pipeline ./cmd/pipeline

generate-fixtures:
	go run ./pkg/testutils/cmd/generate_fixtures

test-functional: generate-fixtures
	go test -v -count=1 ./tests/functional/...

docker-build:
	docker build -t $(IMAGE_NAME):$(VERSION) .

docker-test-functional: generate-fixtures docker-build
	docker compose -f docker-compose.test.yml up -d fake-gcs-server postgres mysql openbao
	docker compose -f docker-compose.test.yml run --rm e2e-happy-path
	docker compose -f docker-compose.test.yml run --rm e2e-compressed-latin1
	docker compose -f docker-compose.test.yml run --rm e2e-dlq-quarantine
	docker compose -f docker-compose.test.yml down


flex-template-build:
	docker build -t $(IMAGE_NAME):$(VERSION) .

flex-template-run:
	gcloud dataflow flex-template run $(JOB_NAME) \
		--template-file-gcs-location=$(TEMPLATE_GCS_PATH) \
		--parameters config_payload='$(CONFIG_PAYLOAD)'

clean:
	rm -rf bin/ testdata/output/
