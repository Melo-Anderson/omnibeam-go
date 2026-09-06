package testutils_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/pkg/testutils"
)

func TestGenerateCSVFixture(t *testing.T) {
	var buf bytes.Buffer
	err := testutils.GenerateCSVFixture(&buf, 5)
	if err != nil {
		t.Fatalf("failed generating CSV fixture: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 6 { // header + 5 rows
		t.Fatalf("expected 6 lines, got %d", len(lines))
	}

	expectedHeader := "id,customer_id,amount,status,created_at"
	if strings.TrimSpace(lines[0]) != expectedHeader {
		t.Errorf("expected header %q, got %q", expectedHeader, lines[0])
	}

	expectedFirstRow := "1,CUST-000001,101.50,PENDING,2026-08-18T10:00:00Z"
	if strings.TrimSpace(lines[1]) != expectedFirstRow {
		t.Errorf("expected first row %q, got %q", expectedFirstRow, lines[1])
	}
}
