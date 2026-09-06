package charsets

import (
	"fmt"
	"io"
	"strings"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// WrapNormalizer wraps r with a transcoding reader that converts the given
// charset to UTF-8. Returns r unchanged for UTF-8 (no allocation).
func WrapNormalizer(r io.Reader, charset string) (io.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(charset)) {
	case "", "utf-8", "utf8":
		return r, nil
	case "iso-8859-1", "latin1", "latin-1":
		return transform.NewReader(r, charmap.ISO8859_1.NewDecoder()), nil
	case "windows-1252", "cp1252":
		return transform.NewReader(r, charmap.Windows1252.NewDecoder()), nil
	case "utf-16le":
		return transform.NewReader(r, unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewDecoder()), nil
	case "utf-16be":
		return transform.NewReader(r, unicode.UTF16(unicode.BigEndian, unicode.UseBOM).NewDecoder()), nil
	default:
		return nil, fmt.Errorf("unsupported charset: %s", charset)
	}
}
