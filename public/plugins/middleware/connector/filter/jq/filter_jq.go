package jq

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/itchyny/gojq"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

func init() {
	if err := cmw.Register("filter_jq", newFilterJQMiddleware); err != nil {
		panic(fmt.Sprintf("register filter_jq connector middleware: %v", err))
	}
}

type filterJQMiddleware struct {
	produceCode *gojq.Code
	consumeCode *gojq.Code
	l           *slog.Logger
}

func newFilterJQMiddleware(config any, l *slog.Logger) (cmw.Middleware, error) {
	var produceExpr, consumeExpr string

	if m, ok := config.(map[string]any); ok {
		if v, ok := m["produce"].(string); ok {
			produceExpr = v
		}
		if v, ok := m["consume"].(string); ok {
			consumeExpr = v
		}
	}

	if produceExpr == "" && consumeExpr == "" {
		return nil, fmt.Errorf("filter_jq middleware: at least one of 'produce' or 'consume' jq expression is required")
	}

	var produceCode, consumeCode *gojq.Code
	var err error

	if produceExpr != "" {
		produceCode, err = compileJQ(produceExpr)
		if err != nil {
			return nil, fmt.Errorf("filter_jq middleware: compile produce expression: %w", err)
		}
	}

	if consumeExpr != "" {
		consumeCode, err = compileJQ(consumeExpr)
		if err != nil {
			return nil, fmt.Errorf("filter_jq middleware: compile consume expression: %w", err)
		}
	}

	l.Info("jq filter middleware initialized",
		"has_produce", produceCode != nil,
		"has_consume", consumeCode != nil,
	)

	return &filterJQMiddleware{
		produceCode: produceCode,
		consumeCode: consumeCode,
		l:           l,
	}, nil
}

func compileJQ(expr string) (*gojq.Code, error) {
	query, err := gojq.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("parse jq expression %q: %w", expr, err)
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return nil, fmt.Errorf("compile jq expression %q: %w", expr, err)
	}
	return code, nil
}

// evaluate runs the jq expression against the message and returns whether the message passes the filter.
func evaluate(code *gojq.Code, msg []byte) (bool, error) {
	var input any
	if err := json.Unmarshal(msg, &input); err != nil {
		return false, fmt.Errorf("message is not valid JSON: %w", err)
	}

	iter := code.Run(input)
	result, ok := iter.Next()
	if !ok {
		return false, nil
	}
	if err, isErr := result.(error); isErr {
		return false, fmt.Errorf("jq expression failed: %w", err)
	}

	return isTruthy(result), nil
}

// isTruthy converts a jq result to boolean following jq semantics:
// false and null are falsy, everything else is truthy.
func isTruthy(v any) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	default:
		return true
	}
}

func (m *filterJQMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	if m.produceCode == nil {
		return w
	}
	return &filterWriterWrapper{
		w:  w,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

func (m *filterJQMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	if m.consumeCode == nil {
		return r
	}
	return &filterReaderWrapper{
		r:  r,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

// FilteredError indicates a message was filtered out.
type FilteredError struct{}

func (e *FilteredError) Error() string {
	return "message filtered out by jq expression"
}
