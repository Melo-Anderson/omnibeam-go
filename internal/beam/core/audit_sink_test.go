package core

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

		"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestAuditSinkDoFn_WritesJSONL(t *testing.T) {
	storage := &fakeStorageWriter{}
	fn := NewAuditSinkDoFn(storage, "gs://bucket/audit")

	ctx := context.Background()
	// apache-beam-practices.md §6: always call Setup before ProcessElement in unit tests.
	if err := fn.Setup(ctx); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	rec := domain.NewAuditRecord("type_cast_failure", domain.SeverityError, "id", "type_cast", "cannot cast", "abc")
	if err := fn.ProcessElement(ctx, rec); err != nil {
		t.Fatalf("ProcessElement: %v", err)
	}

	if err := fn.FinishBundle(ctx); err != nil {
		t.Fatalf("FinishBundle: %v", err)
	}

	if !storage.tempCommitted {
		t.Fatal("expected CommitTemp to be called")
	}

	written := storage.buf.String()
	if !strings.Contains(written, `"event_type":"type_cast_failure"`) {
		t.Errorf("JSONL output missing event_type: %s", written)
	}
	if !strings.HasSuffix(strings.TrimSpace(written), "}") {
		t.Errorf("expected JSONL to end with a JSON object: %s", written)
	}

	// Verify each line is valid JSON.
	for _, line := range strings.Split(strings.TrimSpace(written), "\n") {
		var out domain.AuditRecord
		if err := json.Unmarshal([]byte(line), &out); err != nil {
			t.Errorf("invalid JSON line %q: %v", line, err)
		}
	}
}

func TestAuditSinkDoFn_EmptyBundle_AbortsFile(t *testing.T) {
	storage := &fakeStorageWriter{}
	fn := NewAuditSinkDoFn(storage, "gs://bucket/audit")

	ctx := context.Background()
	if err := fn.Setup(ctx); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	// No ProcessElement calls — empty bundle.
	if err := fn.FinishBundle(ctx); err != nil {
		t.Fatalf("FinishBundle: %v", err)
	}

	// No records written → CommitTemp must NOT have been called (lazy-open: no file was created).
	if storage.tempCommitted {
		t.Error("expected no CommitTemp for empty bundle")
	}
}
