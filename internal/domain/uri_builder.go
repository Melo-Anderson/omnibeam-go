package domain

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// BuildOutputURI resolves whether baseURI is an explicit file or a directory,
// generating clean, deterministic shard names or single file outputs.
func BuildOutputURI(baseURI, ext, compression string, singleFile bool) string {
	baseURI = strings.TrimRight(baseURI, "/")

	// If the user supplied an explicit filename with extension (e.g. data.parquet, output.csv, log.jsonl)
	if filepath.Ext(baseURI) != "" {
		return baseURI
	}

	fullExt := strings.TrimPrefix(ext, ".")
	fullExt = OrDefault(fullExt, DefaultDestinationOutputFormat)
	switch strings.ToLower(compression) {
	case "gzip":
		fullExt += ".gz"
	case "zstd":
		fullExt += ".zst"
	}

	if singleFile {
		return fmt.Sprintf("%s/output.%s", baseURI, fullExt)
	}

	shardID := uuid.New().String()[:8]
	return fmt.Sprintf("%s/shard-%s-%d.%s", baseURI, shardID, time.Now().UnixNano(), fullExt)
}
