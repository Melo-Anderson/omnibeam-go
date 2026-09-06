//go:build integration
// +build integration

package functional_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	gcs_client "cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func setupFakeGCSServer(t *testing.T) *gcs_client.Client {
	emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST")
	if emulatorHost == "" {
		emulatorHost = "127.0.0.1:4443"
		_ = os.Setenv("STORAGE_EMULATOR_HOST", emulatorHost)
	}

	conn, err := net.DialTimeout("tcp", emulatorHost, 500*time.Millisecond)
	if err != nil {
		t.Skipf("skipping GCS test: fake-gcs-server not reachable at %s", emulatorHost)
	}
	_ = conn.Close()

	endpoint := fmt.Sprintf("http://%s/storage/v1/", emulatorHost)
	ctx := context.Background()
	client, err := gcs_client.NewClient(ctx, option.WithEndpoint(endpoint), option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("failed connecting to fake-gcs-server at %s: %v", endpoint, err)
	}

	// Ensure test bucket exists
	bucket := client.Bucket("test-bucket")
	_ = bucket.Create(ctx, "test-project", nil)

	return client
}

func uploadFixtureToGCS(t *testing.T, client *gcs_client.Client, localPath, gcsObject string) {
	ctx := context.Background()
	data, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("failed reading local fixture %s: %v", localPath, err)
	}

	w := client.Bucket("test-bucket").Object(gcsObject).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		t.Fatalf("failed writing fixture to gcs object %s: %v", gcsObject, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed closing gcs writer for %s: %v", gcsObject, err)
	}
}

func TestE2E_GCS_AllScenarios(t *testing.T) {
	binPath := testPipelineBin

	client := setupFakeGCSServer(t)
	defer client.Close()

	// Seed test input files into fake-gcs-server
	cleanFixture := filepath.Join(repoRoot, "testdata", "fixtures", "clean_sample.csv")
	malformedFixture := filepath.Join(repoRoot, "testdata", "fixtures", "malformed_sample.csv")
	uploadFixtureToGCS(t, client, cleanFixture, "input/clean_sample.csv")
	uploadFixtureToGCS(t, client, malformedFixture, "input/malformed_sample.csv")

	t.Run("E2E-GCS-01: GCS CSV to GCS Parquet", func(t *testing.T) {
		cfgPath := filepath.Join(repoRoot, "testdata", "configs", "manifest_gcs_csv_to_parquet.json")
		cmd := exec.Command(binPath, "--config", cfgPath)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "STORAGE_EMULATOR_HOST=127.0.0.1:4443")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("pipeline failed: %v\nOutput:\n%s", err, string(out))
		}

		ctx := context.Background()
		r, err := client.Bucket("test-bucket").Object("output/e2e-gcs-01/clean.parquet").NewReader(ctx)
		if err != nil {
			t.Fatalf("failed opening output parquet from gcs: %v", err)
		}
		defer r.Close()

		data, err := io.ReadAll(r)
		if err != nil || len(data) == 0 {
			t.Fatalf("expected non-empty parquet data from gcs, got len %d, err: %v", len(data), err)
		}
	})

	t.Run("E2E-GCS-02: PostgreSQL to GCS Parquet via Parallel Slices", func(t *testing.T) {
		cfgPath := filepath.Join(repoRoot, "testdata", "configs", "manifest_gcs_sql_to_parquet.json")
		cmd := exec.Command(binPath, "--config", cfgPath)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "STORAGE_EMULATOR_HOST=127.0.0.1:4443")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("pipeline failed: %v\nOutput:\n%s", err, string(out))
		}

		ctx := context.Background()
		r, err := client.Bucket("test-bucket").Object("output/e2e-gcs-02/orders_sql.parquet").NewReader(ctx)
		if err != nil {
			t.Fatalf("failed opening sql output parquet from gcs: %v", err)
		}
		defer r.Close()

		data, err := io.ReadAll(r)
		if err != nil || len(data) == 0 {
			t.Fatalf("expected non-empty sql output parquet from gcs, got len %d, err: %v", len(data), err)
		}
	})

	t.Run("E2E-GCS-03: GCS Malformed File to GCS Parquet and DLQ Quarantine", func(t *testing.T) {
		cfgPath := filepath.Join(repoRoot, "testdata", "configs", "manifest_gcs_dlq_quarantine.json")
		cmd := exec.Command(binPath, "--config", cfgPath)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "STORAGE_EMULATOR_HOST=127.0.0.1:4443")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("pipeline failed: %v\nOutput:\n%s", err, string(out))
		}

		ctx := context.Background()
		dlqReader, err := client.Bucket("test-bucket").Object("output/e2e-gcs-03/dlq.jsonl").NewReader(ctx)
		if err != nil {
			t.Fatalf("failed opening dlq quarantine from gcs: %v", err)
		}
		defer dlqReader.Close()

		dlqData, err := io.ReadAll(dlqReader)
		if err != nil || len(dlqData) == 0 {
			t.Fatalf("expected non-empty dlq output from gcs, got len %d, err: %v", len(dlqData), err)
		}
	})
}
