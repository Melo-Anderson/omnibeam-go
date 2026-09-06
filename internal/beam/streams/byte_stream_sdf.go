package streams

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*ByteOffsetRange)(nil)).Elem())
	beam.RegisterDoFn(reflect.TypeOf((*ByteStreamSourceSDF)(nil)).Elem())
}

// Compile-time assertion: ByteStreamSourceSDF must implement the SDF lifecycle visible to the Beam runner.
// If the Beam SDK changes the required SDF method set, this will break the build immediately.
var _ interface {
	CreateInitialRestriction(context.Context, string) (ByteOffsetRange, error)
	SplitRestriction(string, ByteOffsetRange) []ByteOffsetRange
	RestrictionSize(string, ByteOffsetRange) float64
	CreateTracker(ByteOffsetRange) *ByteOffsetTracker
	ProcessElement(context.Context, *ByteOffsetTracker, string, func(*domain.GenericRecord)) error
} = (*ByteStreamSourceSDF)(nil)

// ByteStreamSourceSDF is a Splittable DoFn that reads file/stream byte chunks using ByteOffsetTracker.
type ByteStreamSourceSDF struct {
	SourceCfg     domain.SourceConfig `json:"source_cfg"`
	reader        ports.StorageReader
	streamWrapper ports.StreamWrapper
	decoder       ports.StreamDecoder
}

// NewByteStreamSourceSDF creates a new ByteStreamSourceSDF DoFn with injected port dependencies.
func NewByteStreamSourceSDF(
	reader ports.StorageReader,
	streamWrapper ports.StreamWrapper,
	decoder ports.StreamDecoder,
	sourceCfg domain.SourceConfig,
) *ByteStreamSourceSDF {
	return &ByteStreamSourceSDF{
		reader:        reader,
		streamWrapper: streamWrapper,
		decoder:       decoder,
		SourceCfg:     sourceCfg,
	}
}

func (fn *ByteStreamSourceSDF) ensureReader(fileURI string) error {
	if fn.reader == nil && globalStorageReaderProvider != nil {
		fn.reader = globalStorageReaderProvider(fileURI)
	}
	if fn.reader == nil {
		return fmt.Errorf("storage reader is not initialized")
	}
	return nil
}

func (fn *ByteStreamSourceSDF) ensureDecoderAndWrapper() error {
	if fn.decoder == nil && globalStreamDecoderProvider != nil {
		fn.decoder = globalStreamDecoderProvider(&fn.SourceCfg)
	}
	if fn.decoder == nil {
		return fmt.Errorf("stream decoder is not initialized")
	}
	if fn.streamWrapper == nil && globalStreamWrapperProvider != nil {
		fn.streamWrapper = globalStreamWrapperProvider(&fn.SourceCfg)
	}
	return nil
}

// CreateInitialRestriction creates the initial byte-offset restriction using O(1) Size query.
// This replaces the previous stream-drain approach that downloaded entire files just to measure size.
func (fn *ByteStreamSourceSDF) CreateInitialRestriction(ctx context.Context, fileURI string) (ByteOffsetRange, error) {
	if err := fn.ensureReader(fileURI); err != nil {
		return ByteOffsetRange{}, err
	}

	size, err := fn.reader.Size(ctx, fileURI)
	if err != nil {
		return ByteOffsetRange{}, fmt.Errorf("failed getting size for initial restriction on %q: %w", fileURI, err)
	}

	return ByteOffsetRange{Start: 0, End: size}, nil
}

// SplitRestriction splits the initial byte-offset restriction into parallel chunk intervals.
// Compressed files (gzip, zstd, snappy, bzip2) cannot be byte-split: their decompression
// stream requires the file header at byte 0. They are kept as a single restriction and
// processed by one worker; downstream Beam transforms (validation, casting) still run distributed.
func (fn *ByteStreamSourceSDF) SplitRestriction(_ string, rest ByteOffsetRange) []ByteOffsetRange {
	comp := strings.ToLower(strings.TrimSpace(fn.SourceCfg.Compression))
	if comp != "" && comp != "none" {
		return []ByteOffsetRange{rest}
	}

	chunkSize := fn.SourceCfg.ChunkSizeBytes
	if rest.End-rest.Start <= chunkSize {
		return []ByteOffsetRange{rest}
	}

	var splits []ByteOffsetRange
	for start := rest.Start; start < rest.End; start += chunkSize {
		end := start + chunkSize
		if end > rest.End {
			end = rest.End
		}
		splits = append(splits, ByteOffsetRange{Start: start, End: end})
	}
	return splits
}

// RestrictionSize computes the byte length of the restriction.
func (fn *ByteStreamSourceSDF) RestrictionSize(_ string, rest ByteOffsetRange) float64 {
	if rest.End < rest.Start {
		return 0
	}
	return float64(rest.End - rest.Start)
}

// CreateTracker returns a new ByteOffsetTracker initialized with the given restriction.
func (fn *ByteStreamSourceSDF) CreateTracker(rest ByteOffsetRange) *ByteOffsetTracker {
	return NewByteOffsetTracker(rest)
}

// ProcessElement reads the byte range assigned to this worker, claims each line
// position via ByteOffsetTracker, feeds lines through an io.Pipe to the StreamDecoder,
// and emits decoded GenericRecords downstream.
func (fn *ByteStreamSourceSDF) ProcessElement(
	ctx context.Context,
	tracker *ByteOffsetTracker,
	fileURI string,
	emit func(*domain.GenericRecord),
) error {
	rest, ok := tracker.GetRestriction().(ByteOffsetRange)
	if !ok {
		return fmt.Errorf("unexpected restriction type %T", tracker.GetRestriction())
	}
	defer tracker.MarkDone()

	if err := fn.ensureReader(fileURI); err != nil {
		return err
	}
	if err := fn.ensureDecoderAndWrapper(); err != nil {
		return err
	}

	rc, err := fn.reader.Open(ctx, fileURI)
	if err != nil {
		return fmt.Errorf("failed opening %q: %w", fileURI, err)
	}
	defer rc.Close()

	if seeker, ok := rc.(io.ReadSeeker); ok {
		if _, err := seeker.Seek(rest.Start, io.SeekStart); err != nil {
			return fmt.Errorf("failed seeking to offset %d in %q: %w", rest.Start, fileURI, err)
		}
	} else if rest.Start > 0 {
		if _, err := io.CopyN(io.Discard, rc, rest.Start); err != nil {
			return fmt.Errorf("failed discarding %d bytes in %q: %w", rest.Start, fileURI, err)
		}
	}

	br := bufio.NewReaderSize(rc, 64*1024)
	currPos := rest.Start

	quoteChar := byte('"')
	if len(fn.SourceCfg.QuoteChar) > 0 {
		quoteChar = fn.SourceCfg.QuoteChar[0]
	}
	multiline := fn.SourceCfg.Multiline

	// If not starting at offset 0, discard partial line respecting quote parity
	if rest.Start > 0 {
		line, err := readRecordBytes(br, quoteChar, multiline)
		currPos += int64(len(line))
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed seeking record boundary in %q: %w", fileURI, err)
		}
		if currPos >= rest.End {
			return nil
		}
	}

	pr, pw := io.Pipe()
	feedErrChan := make(chan error, 1)

	// Ensure the write end of the pipe is always closed so the decoder doesn't hang.
	go func() {
		defer close(feedErrChan)
		defer pw.Close()
		for {
			select {
			case <-ctx.Done():
				feedErrChan <- ctx.Err()
				return
			default:
			}

			if currPos >= rest.End {
				return
			}
			if !tracker.ClaimOffset(currPos) {
				return
			}

			line, err := readRecordBytes(br, quoteChar, multiline)
			if len(line) > 0 {
				if _, wErr := pw.Write(line); wErr != nil {
					if wErr != io.ErrClosedPipe {
						feedErrChan <- wErr
					}
					return
				}
				currPos += int64(len(line))
			}
			if err != nil {
				if err != io.EOF {
					feedErrChan <- err
				}
				return
			}
		}
	}()

	// Guarantee read pipe is closed on exit to unblock the feeder goroutine
	defer func() { _ = pr.CloseWithError(context.Canceled) }()

	var stream io.Reader = pr
	if fn.streamWrapper != nil {
		wrapped, wErr := fn.streamWrapper.WrapStream(pr, &fn.SourceCfg)
		if wErr != nil {
			_ = pr.CloseWithError(wErr)
			return fmt.Errorf("failed wrapping stream for %q: %w", fileURI, wErr)
		}
		stream = wrapped
	}

	recChan, decErrChan, err := fn.decoder.Decode(ctx, stream, &fn.SourceCfg.Schema, fileURI)
	if err != nil {
		_ = pr.CloseWithError(err)
		return fmt.Errorf("decoder setup error for %q: %w", fileURI, err)
	}

	for rec := range recChan {
		if rec != nil {
			emit(rec)
		}
	}

	for decErr := range decErrChan {
		if decErr != nil {
			return fmt.Errorf("stream read error in %q: %w", fileURI, decErr)
		}
	}

	for feedErr := range feedErrChan {
		if feedErr != nil {
			return fmt.Errorf("feed error in %q: %w", fileURI, feedErr)
		}
	}

	return nil
}

func readRecordBytes(br *bufio.Reader, quoteChar byte, multiline ...bool) ([]byte, error) {
	isMulti := false
	if len(multiline) > 0 {
		isMulti = multiline[0]
	}
	if !isMulti {
		return br.ReadBytes('\n')
	}
	if quoteChar == 0 {
		quoteChar = '"'
	}
	var full []byte
	inQuotes := false

	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			full = append(full, line...)
			for i := 0; i < len(line); i++ {
				b := line[i]
				if b == quoteChar {
					if inQuotes && i+1 < len(line) && line[i+1] == quoteChar {
						i++ // skip doubled quote escape ""
						continue
					}
					if i > 0 && line[i-1] == '\\' {
						continue // skip backslash escape \"
					}
					inQuotes = !inQuotes
				}
			}
			if !inQuotes || err != nil {
				return full, err
			}
		} else {
			return full, err
		}
	}
}
