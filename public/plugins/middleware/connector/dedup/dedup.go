package dedup

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/itchyny/gojq"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

func init() {
	if err := cmw.Register("dedup", newDedupMiddleware); err != nil {
		panic(fmt.Sprintf("register dedup connector middleware: %v", err))
	}
}

type keyStrategy int

const (
	strategyContentHash keyStrategy = iota
	strategyHeader
	strategyJQ
)

type dedupMiddleware struct {
	strategy     keyStrategy
	headerName   string
	jqCode       *gojq.Code
	produceStore *dedupStore
	consumeStore *dedupStore
	l            *slog.Logger
}

func newDedupMiddleware(config any, l *slog.Logger) (cmw.Middleware, error) {
	ttl := 5 * time.Minute
	cleanupInterval := 1 * time.Minute
	keyConfig := "content_hash"

	if m, ok := config.(map[string]any); ok {
		if v, ok := m["ttl"].(string); ok {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("dedup middleware: invalid ttl %q: %w", v, err)
			}
			ttl = d
		}
		if v, ok := m["cleanup_interval"].(string); ok {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("dedup middleware: invalid cleanup_interval %q: %w", v, err)
			}
			cleanupInterval = d
		}
		if v, ok := m["key"].(string); ok {
			keyConfig = v
		}
	}

	mw := &dedupMiddleware{
		produceStore: newDedupStore(ttl, cleanupInterval),
		consumeStore: newDedupStore(ttl, cleanupInterval),
		l:            l,
	}

	switch {
	case keyConfig == "content_hash" || keyConfig == "":
		mw.strategy = strategyContentHash
	case strings.HasPrefix(keyConfig, "header:"):
		headerName := strings.TrimPrefix(keyConfig, "header:")
		if headerName == "" {
			return nil, fmt.Errorf("dedup middleware: header name is empty in key config %q", keyConfig)
		}
		mw.strategy = strategyHeader
		mw.headerName = headerName
	case strings.HasPrefix(keyConfig, "jq:"):
		expr := strings.TrimPrefix(keyConfig, "jq:")
		if expr == "" {
			return nil, fmt.Errorf("dedup middleware: jq expression is empty in key config %q", keyConfig)
		}
		query, err := gojq.Parse(expr)
		if err != nil {
			return nil, fmt.Errorf("dedup middleware: parse jq expression %q: %w", expr, err)
		}
		code, err := gojq.Compile(query)
		if err != nil {
			return nil, fmt.Errorf("dedup middleware: compile jq expression %q: %w", expr, err)
		}
		mw.strategy = strategyJQ
		mw.jqCode = code
	default:
		return nil, fmt.Errorf("dedup middleware: unknown key strategy %q (expected content_hash, header:<name>, or jq:<expr>)", keyConfig)
	}

	l.Info("dedup middleware initialized",
		"ttl", ttl,
		"cleanup_interval", cleanupInterval,
		"key_strategy", keyConfig,
	)

	return mw, nil
}

// keyFromContent computes SHA-256 of the raw payload.
func keyFromContent(msg []byte) [32]byte {
	return sha256.Sum256(msg)
}

// keyFromHeader finds the header by name and hashes its value.
// Headers are [][]byte pairs: [key, value, key, value, ...].
func keyFromHeader(headerName string, headers [][]byte) ([32]byte, bool) {
	for i := 0; i+1 < len(headers); i += 2 {
		if string(headers[i]) == headerName {
			return sha256.Sum256(headers[i+1]), true
		}
	}
	return [32]byte{}, false
}

// keyFromJQ extracts a dedup key by running a jq expression on the JSON payload.
func keyFromJQ(code *gojq.Code, msg []byte) ([32]byte, error) {
	var input any
	if err := json.Unmarshal(msg, &input); err != nil {
		return [32]byte{}, fmt.Errorf("message is not valid JSON: %w", err)
	}
	iter := code.Run(input)
	result, ok := iter.Next()
	if !ok {
		return [32]byte{}, fmt.Errorf("jq expression produced no output")
	}
	if err, isErr := result.(error); isErr {
		return [32]byte{}, fmt.Errorf("jq expression failed: %w", err)
	}
	// Marshal the result to get a stable string representation for hashing.
	b, err := json.Marshal(result)
	if err != nil {
		return [32]byte{}, fmt.Errorf("marshal jq result: %w", err)
	}
	return sha256.Sum256(b), nil
}

func (m *dedupMiddleware) computeKey(msg []byte, headers [][]byte) ([32]byte, error) {
	switch m.strategy {
	case strategyHeader:
		if key, found := keyFromHeader(m.headerName, headers); found {
			return key, nil
		}
		// Fallback to content hash when header is not present.
		return keyFromContent(msg), nil
	case strategyJQ:
		return keyFromJQ(m.jqCode, msg)
	default:
		return keyFromContent(msg), nil
	}
}

func (m *dedupMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	return &dedupWriterWrapper{
		w:  w,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

func (m *dedupMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	return &dedupReaderWrapper{
		r:  r,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

// DuplicateError indicates a message was rejected as a duplicate.
type DuplicateError struct{}

func (e *DuplicateError) Error() string {
	return "duplicate message rejected"
}
