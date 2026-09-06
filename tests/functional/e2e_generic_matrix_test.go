package functional_test

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
	"golang.org/x/text/encoding/charmap"
)

// MatrixScenario defines a declarative input spec for end-to-end pipeline execution.
type MatrixScenario struct {
	Name           string
	SourceKind     string // "file", "rest_api"
	SourceFormat   string // "csv", "jsonl"
	Delimiter      string // ",", ";", "|"
	Charset        string // "UTF-8", "ISO-8859-1"
	Compression    string // "none", "gzip", "zstd"
	Rows           int
	CorruptRows    int
	PaginationType domain.PaginationType // domain.PaginationPageNumber, domain.PaginationOffsetLimit
	DestType       string                // "storage", "rest_api"
	DestFormat     string                // "parquet", "jsonl", "csv"
	DestDelimiter  string                // ",", ";"
}

var stdMatrixSchema = domain.Schema{
	Fields: []domain.Field{
		{Name: "id", Type: domain.TypeInt64, Nullable: false},
		{Name: "name", Type: domain.TypeString, Nullable: false},
		{Name: "amount", Type: domain.TypeFloat64, Nullable: false},
		{Name: "active", Type: domain.TypeBool, Nullable: false},
		{Name: "created_at", Type: domain.TypeTimestamp, Nullable: false},
	},
}

// TestE2E_GenericMatrix_AllSourcesAndDestinations executes a DRY data-driven matrix
// across all combinations of formats, codecs, charsets, and API pagination strategies.
func TestE2E_GenericMatrix_AllSourcesAndDestinations(t *testing.T) {
	scenarios := []MatrixScenario{
		{
			Name:       "Matrix-01: CSV [Comma, UTF-8, Raw] -> Parquet",
			SourceKind: "file", SourceFormat: "csv", Delimiter: ",", Charset: "UTF-8", Rows: 100,
			DestType: "storage", DestFormat: "parquet",
		},
		{
			Name:       "Matrix-02: CSV [Semicolon, ISO-8859-1, Gzip] -> JSONL",
			SourceKind: "file", SourceFormat: "csv", Delimiter: ";", Charset: "ISO-8859-1", Compression: "gzip", Rows: 80,
			DestType: "storage", DestFormat: "jsonl",
		},
		{
			Name:       "Matrix-03: CSV [Pipe, UTF-8, Zstd] -> CSV [Semicolon]",
			SourceKind: "file", SourceFormat: "csv", Delimiter: "|", Charset: "UTF-8", Compression: "zstd", Rows: 50,
			DestType: "storage", DestFormat: "csv", DestDelimiter: ";",
		},
		{
			Name:       "Matrix-04: JSONL [Raw, UTF-8] -> Parquet",
			SourceKind: "file", SourceFormat: "jsonl", Rows: 120,
			DestType: "storage", DestFormat: "parquet",
		},
		{
			Name:       "Matrix-05: JSONL [Zstd] -> JSONL",
			SourceKind: "file", SourceFormat: "jsonl", Compression: "zstd", Rows: 60,
			DestType: "storage", DestFormat: "jsonl",
		},
		{
			Name:       "Matrix-06: REST API [PageNumber] -> Parquet",
			SourceKind: "rest_api", PaginationType: domain.PaginationPageNumber, Rows: 4,
			DestType: "storage", DestFormat: "parquet",
		},
		{
			Name:       "Matrix-07: REST API [OffsetLimit] -> CSV",
			SourceKind: "rest_api", PaginationType: domain.PaginationOffsetLimit, Rows: 4,
			DestType: "storage", DestFormat: "csv", DestDelimiter: ",",
		},
		{
			Name:       "Matrix-08: CSV Ingestion -> Export to REST API Sink",
			SourceKind: "file", SourceFormat: "csv", Delimiter: ",", Rows: 30,
			DestType: "rest_api",
		},
		{
			Name:       "Matrix-09: Corrupt CSV Input -> Valid Parquet + Quarantined DLQ",
			SourceKind: "file", SourceFormat: "csv", Delimiter: ",", Rows: 30, CorruptRows: 10,
			DestType: "storage", DestFormat: "parquet",
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			runMatrixScenario(t, sc)
		})
	}
}

func runMatrixScenario(t *testing.T, sc MatrixScenario) {
	tmpDir := t.TempDir()
	dlqPath := filepath.Join(tmpDir, "dlq", "quarantine.jsonl")

	srcCfg, apiCfg, cleanupSrc := buildScenarioSource(t, sc, tmpDir)
	if cleanupSrc != nil {
		defer cleanupSrc()
	}

	destCfg, verifyDest := buildScenarioDest(t, sc, tmpDir)

	pipeCfg := &domain.PipelineConfig{
		PipelineID:  "matrix-pipeline",
		RunID:       fmt.Sprintf("run-%d", time.Now().UnixNano()),
		Source:      srcCfg,
		APISource:   apiCfg,
		Destination: destCfg,
		DLQConfig:   domain.DLQConfig{QuarantinePath: dlqPath},
	}

	manifestPath := filepath.Join(tmpDir, "manifest.json")
	manifestBytes, _ := json.Marshal(pipeCfg)
	_ = os.WriteFile(manifestPath, manifestBytes, 0644)

	cmd := exec.Command(testPipelineBin, "--config="+manifestPath, "--runner=direct")
	cmd.Dir = repoRoot
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pipeline execution failed: %v\nOutput:\n%s", err, string(output))
	}

	verifyDest(t, sc.Rows)
	if sc.CorruptRows > 0 {
		verifyDLQOutput(t, dlqPath, sc.CorruptRows)
	}
}

func buildScenarioSource(t *testing.T, sc MatrixScenario, tmpDir string) (domain.SourceConfig, *domain.APISourceConfig, func()) {
	if sc.SourceKind == "rest_api" {
		ts := startMockAPIServer(t, sc.PaginationType)
		return domain.SourceConfig{}, &domain.APISourceConfig{
			BaseURL:     ts.URL,
			Endpoint:    "/items",
			RecordsPath: "items",
			Pagination: domain.APIPaginationConfig{
				Type:           sc.PaginationType,
				PageParam:      selectParam(sc.PaginationType == domain.PaginationPageNumber, "page", "offset"),
				SizeParam:      selectParam(sc.PaginationType == domain.PaginationPageNumber, "size", "limit"),
				PageSize:       2,
				TotalPagesHint: 2,
			},
			Schema: stdMatrixSchema,
		}, ts.Close
	}

	ext := "." + sc.SourceFormat
	if sc.Compression == "gzip" {
		ext += ".gz"
	} else if sc.Compression == "zstd" {
		ext += ".zst"
	}
	inPath := filepath.Join(tmpDir, "input"+ext)

	if sc.SourceFormat == "csv" {
		delim := ','
		if len(sc.Delimiter) > 0 {
			delim = rune(sc.Delimiter[0])
		}
		writeCSVFixture(t, inPath, delim, sc.Charset, sc.Compression, sc.Rows, sc.CorruptRows)
	} else {
		writeJSONLFixture(t, inPath, sc.Compression, sc.Rows)
	}

	return domain.SourceConfig{
		Path:        inPath,
		Format:      sc.SourceFormat,
		Delimiter:   sc.Delimiter,
		Charset:     sc.Charset,
		Compression: sc.Compression,
		Schema:      stdMatrixSchema,
	}, nil, nil
}

func buildScenarioDest(t *testing.T, sc MatrixScenario, tmpDir string) (domain.DestinationConfig, func(t *testing.T, count int)) {
	if sc.DestType == "rest_api" {
		var receivedCount int64
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			var batch []map[string]any
			if err := json.Unmarshal(body, &batch); err == nil {
				atomic.AddInt64(&receivedCount, int64(len(batch)))
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		}))
		t.Cleanup(ts.Close)

		return domain.DestinationConfig{
			Type:     "rest_api",
			Endpoint: domain.APIEndpointConfig{BaseURL: ts.URL},
			APIOptions: domain.APISinkOptions{
				ResourcePath: "/batch",
				Method:       "POST",
				BatchSize:    10,
			},
		}, func(t *testing.T, count int) {
			if int(atomic.LoadInt64(&receivedCount)) != count {
				t.Errorf("expected %d records in API sink, got %d", count, atomic.LoadInt64(&receivedCount))
			}
		}
	}

	outDir := filepath.Join(tmpDir, "output")
	outPath := filepath.Join(outDir, "data."+sc.DestFormat)
	destCfg := domain.DestinationConfig{
		Type:         "storage",
		OutputFormat: sc.DestFormat,
		OutputPath:   outPath,
	}
	if sc.DestFormat == "csv" && sc.DestDelimiter != "" {
		destCfg.FormatOptions = domain.FormatOptions{Delimiter: sc.DestDelimiter, IncludeHeader: true}
	}

	return destCfg, func(t *testing.T, count int) {
		switch sc.DestFormat {
		case "parquet":
			verifyParquetOutput(t, outDir, count)
		case "jsonl":
			verifyJSONLOutput(t, outDir, count)
		case "csv":
			delim := ','
			if len(sc.DestDelimiter) > 0 {
				delim = rune(sc.DestDelimiter[0])
			}
			verifyCSVOutput(t, outDir, delim, count)
		}
	}
}

func startMockAPIServer(_ *testing.T, pagType domain.PaginationType) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		pVal, _ := strconv.Atoi(r.URL.Query().Get(selectParam(pagType == domain.PaginationPageNumber, "page", "offset")))
		isPage1 := (pagType == domain.PaginationPageNumber && (pVal <= 1)) || (pagType == domain.PaginationOffsetLimit && pVal == 0)

		if isPage1 {
			w.Write([]byte(`{"items":[{"id":1,"name":"A","amount":10.5,"active":true,"created_at":"2026-08-20T10:00:00Z"},{"id":2,"name":"B","amount":20.0,"active":false,"created_at":"2026-08-20T11:00:00Z"}]}`))
		} else {
			w.Write([]byte(`{"items":[{"id":3,"name":"C","amount":30.5,"active":true,"created_at":"2026-08-20T12:00:00Z"},{"id":4,"name":"D","amount":40.0,"active":false,"created_at":"2026-08-20T13:00:00Z"}]}`))
		}
	}))
}

func selectParam(condition bool, a, b string) string {
	if condition {
		return a
	}
	return b
}

func writeCSVFixture(t *testing.T, path string, delimiter rune, charset, compression string, rows, corruptRows int) {
	t.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Comma = delimiter
	_ = w.Write([]string{"id", "name", "amount", "active", "created_at"})

	for i := 1; i <= rows; i++ {
		_ = w.Write([]string{strconv.Itoa(i), fmt.Sprintf("Item %d", i), fmt.Sprintf("%.2f", float64(i)*10.5), "true", "2026-08-20T10:00:00Z"})
	}
	for i := 1; i <= corruptRows; i++ {
		_ = w.Write([]string{strconv.Itoa(rows + i), "Corrupt", "INVALID_FLOAT", "true", "INVALID_TIMESTAMP"})
	}
	w.Flush()

	raw := buf.Bytes()
	if charset == "ISO-8859-1" {
		raw, _ = charmap.ISO8859_1.NewEncoder().Bytes(raw)
	}
	compressAndSave(t, path, raw, compression)
}

func writeJSONLFixture(t *testing.T, path string, compression string, rows int) {
	t.Helper()
	var buf bytes.Buffer
	for i := 1; i <= rows; i++ {
		data, _ := json.Marshal(map[string]any{
			"id": i, "name": fmt.Sprintf("Item %d", i), "amount": float64(i) * 10.5, "active": true, "created_at": "2026-08-20T10:00:00Z",
		})
		buf.Write(data)
		buf.WriteByte('\n')
	}
	compressAndSave(t, path, buf.Bytes(), compression)
}

func compressAndSave(t *testing.T, path string, raw []byte, compression string) {
	t.Helper()
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed creating fixture %s: %v", path, err)
	}
	defer f.Close()

	switch compression {
	case "gzip":
		gz := gzip.NewWriter(f)
		_, _ = gz.Write(raw)
		_ = gz.Close()
	case "zstd":
		enc, _ := zstd.NewWriter(f)
		_, _ = enc.Write(raw)
		_ = enc.Close()
	default:
		_, _ = f.Write(raw)
	}
}

func verifyParquetOutput(t *testing.T, outDir string, expectedRows int) {
	t.Helper()
	entries, _ := os.ReadDir(outDir)
	var totalRows int64
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".parquet" {
			f, err := os.Open(filepath.Join(outDir, entry.Name()))
			if err != nil {
				t.Fatalf("failed opening parquet file: %v", err)
			}
			fi, _ := f.Stat()
			pf, err := parquet.OpenFile(f, fi.Size())
			if err == nil {
				totalRows += pf.NumRows()
			}
			f.Close()
		}
	}
	if int(totalRows) != expectedRows {
		t.Errorf("expected %d parquet rows in %s, got %d", expectedRows, outDir, totalRows)
	}
}

func verifyJSONLOutput(t *testing.T, outDir string, expectedRows int) {
	t.Helper()
	entries, _ := os.ReadDir(outDir)
	var totalRows int
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".jsonl" && entry.Name() != "metrics.json" {
			content, _ := os.ReadFile(filepath.Join(outDir, entry.Name()))
			for _, line := range bytes.Split(bytes.TrimSpace(content), []byte("\n")) {
				if len(bytes.TrimSpace(line)) > 0 {
					totalRows++
				}
			}
		}
	}
	if totalRows != expectedRows {
		t.Errorf("expected %d JSONL rows in %s, got %d", expectedRows, outDir, totalRows)
	}
}

func verifyCSVOutput(t *testing.T, outDir string, delimiter rune, expectedRows int) {
	t.Helper()
	entries, _ := os.ReadDir(outDir)
	var totalRows int
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".csv" {
			f, err := os.Open(filepath.Join(outDir, entry.Name()))
			if err == nil {
				r := csv.NewReader(f)
				r.Comma = delimiter
				records, _ := r.ReadAll()
				f.Close()
				if len(records) > 0 {
					totalRows += len(records) - 1
				}
			}
		}
	}
	if totalRows != expectedRows {
		t.Errorf("expected %d CSV rows in %s, got %d", expectedRows, outDir, totalRows)
	}
}

func verifyDLQOutput(t *testing.T, dlqPath string, expectedDLQ int) {
	t.Helper()
	content, err := os.ReadFile(dlqPath)
	if err != nil {
		t.Fatalf("failed reading DLQ file: %v", err)
	}
	var count int
	for _, line := range bytes.Split(bytes.TrimSpace(content), []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			count++
		}
	}
	if count != expectedDLQ {
		t.Errorf("expected %d DLQ records, got %d", expectedDLQ, count)
	}
}
