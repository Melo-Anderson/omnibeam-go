package domain

import (
	"strings"
)

type SourceConfig struct {
	Type           string   `json:"type"`
	Path           string   `json:"path,omitempty"`
	Paths          []string `json:"paths,omitempty"`
	Format         string   `json:"format"`                      // "csv", "jsonl"
	Delimiter      string   `json:"delimiter"`                   // ",", ";", "|", "\t"
	QuoteChar      string   `json:"quote_char,omitempty"`        // "\"", "'", etc.
	Multiline      bool     `json:"multiline,omitempty"`         // true for CSVs with newlines inside quotes
	Charset        string   `json:"charset"`                     // "utf-8", "iso-8859-1", "windows-1252"
	Compression    string   `json:"compression"`                 // "none", "gzip", "snappy", "zstd", "bzip2"
	ChunkSizeBytes int64    `json:"chunk_size_bytes,omitempty"` // Custom chunk size for ByteStream offset splitting
	Schema         Schema   `json:"schema"`
}

// AllPaths returns the list of target file paths configured for the source.
// It prioritizes explicit Paths if populated; otherwise falls back to single Path.
func (s *SourceConfig) AllPaths() []string {
	if len(s.Paths) > 0 {
		result := make([]string, 0, len(s.Paths))
		for _, p := range s.Paths {
			p = strings.TrimSpace(p)
			if p != "" {
				result = append(result, p)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	if strings.TrimSpace(s.Path) != "" {
		return []string{strings.TrimSpace(s.Path)}
	}
	return nil
}

// ApplyDefaults populates default values for omitted fields in SourceConfig.
func (s *SourceConfig) ApplyDefaults() {
	s.Format = strings.ToLower(strings.TrimSpace(s.Format))
	s.Format = OrDefault(s.Format, DefaultSourceFormat)
	s.Delimiter = OrDefault(s.Delimiter, DefaultSourceDelimiter)
	s.QuoteChar = OrDefault(s.QuoteChar, DefaultSourceQuoteChar)
	s.Charset = strings.ToLower(strings.TrimSpace(s.Charset))
	s.Charset = OrDefault(s.Charset, DefaultSourceCharset)
	s.Compression = strings.ToLower(strings.TrimSpace(s.Compression))
	s.Compression = OrDefault(s.Compression, DefaultSourceCompression)
	s.ChunkSizeBytes = OrDefault(s.ChunkSizeBytes, DefaultChunkSizeBytes)
	s.Schema.ApplyDefaults()
}
