package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

func isRemoteURI(path string) bool {
	return strings.HasPrefix(path, "gs://") || strings.HasPrefix(path, "gs:\\")
}

// writeErrorFile creates error.txt containing the failure message in the output directory.
// It is best-effort: failures to write are logged and never mask the original cause.
func writeErrorFile(outDir string, cause error) {
	if cause == nil || outDir == "" || isRemoteURI(outDir) {
		return
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		slog.Warn("failed creating error file directory", "dir", outDir, "error", err)
		return
	}
	target := filepath.Join(outDir, "error.txt")
	if err := os.WriteFile(target, []byte(cause.Error()), 0644); err != nil {
		slog.Warn("failed writing error file", "path", target, "error", err)
	}
}
