package domain

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// BuildOutputURI resolves the final output URI for a sink shard.
// Extensions are composed in order: format → compression → encryption.
// Example: BuildOutputURI("/out", "csv", "gzip", "pgp", false)
//
//	→ "/out/shard-abc12345-1234567890.csv.gz.pgp"
func BuildOutputURI(baseURI, ext, compression, encryption string, singleFile bool) string {
	baseURI = strings.TrimRight(baseURI, "/")

	// If the caller supplied an explicit file path with extension, honor it as-is.
	if filepath.Ext(baseURI) != "" {
		return baseURI
	}

	fullExt := strings.TrimPrefix(strings.ToLower(ext), ".")
	fullExt = OrDefault(fullExt, DefaultDestinationOutputFormat)

	switch strings.ToLower(compression) {
	case "gzip":
		fullExt += ".gz"
	case "zstd":
		fullExt += ".zst"
	case "snappy":
		fullExt += ".snappy"
	}

	switch strings.ToLower(encryption) {
	case "pgp", "gpg":
		fullExt += ".pgp"
	case "kms":
		fullExt += ".enc"
	}

	if singleFile {
		return fmt.Sprintf("%s/output.%s", baseURI, fullExt)
	}

	shardID := uuid.New().String()[:8]
	return fmt.Sprintf("%s/shard-%s-%d.%s", baseURI, shardID, time.Now().UnixNano(), fullExt)
}
