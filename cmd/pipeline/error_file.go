package main

import (
	"os"
	"path/filepath"
	"strings"
)

// writeErrorFile creates error.txt containing the failure message in the output directory.
func writeErrorFile(outDir string, cause error) {
	if cause == nil || outDir == "" || strings.HasPrefix(outDir, "gs://") || strings.HasPrefix(outDir, "gs:\\") {
		return
	}
	_ = os.MkdirAll(outDir, 0755)
	_ = os.WriteFile(filepath.Join(outDir, "error.txt"), []byte(cause.Error()), 0644)
}
