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
	if err := cmw.Register("transform_jq", newTransformJQMiddleware); err != nil {
		panic(fmt.Sprintf("register transform_jq connector middleware: %v", err))
	}
}

type transformJQMiddleware struct {
	produceCode *gojq.Code
	consumeCode *gojq.Code
	l           *slog.Logger
}

func newTransformJQMiddleware(config any, l *slog.Logger) (cmw.Middleware, error) {
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
		return nil, fmt.Errorf("transform_jq middleware: at least one of 'produce' or 'consume' jq expression is required")
	}

	var produceCode, consumeCode *gojq.Code
	var err error

	if produceExpr != "" {
		produceCode, err = compileJQ(produceExpr)
		if err != nil {
			return nil, fmt.Errorf("transform_jq middleware: compile produce expression: %w", err)
		}
	}

	if consumeExpr != "" {
		consumeCode, err = compileJQ(consumeExpr)
		if err != nil {
			return nil, fmt.Errorf("transform_jq middleware: compile consume expression: %w", err)
		}
	}

	l.Info("jq transform middleware initialized",
		"has_produce", produceCode != nil,
		"has_consume", consumeCode != nil,
	)

	return &transformJQMiddleware{
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

func transform(code *gojq.Code, msg []byte) ([]byte, error) {
	var input any
	if err := json.Unmarshal(msg, &input); err != nil {
		return nil, fmt.Errorf("message is not valid JSON: %w", err)
	}

	iter := code.Run(input)
	result, ok := iter.Next()
	if !ok {
		return nil, fmt.Errorf("jq expression produced no output")
	}
	if err, isErr := result.(error); isErr {
		return nil, fmt.Errorf("jq expression failed: %w", err)
	}

	out, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal transformed result: %w", err)
	}
	return out, nil
}

func (m *transformJQMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	if m.produceCode == nil {
		return w
	}
	return &jqWriterWrapper{
		w:  w,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

func (m *transformJQMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	if m.consumeCode == nil {
		return r
	}
	return &jqReaderWrapper{
		r:  r,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

// TransformError wraps a jq transformation failure.
type TransformError struct {
	Err error
}

func (e *TransformError) Error() string {
	return fmt.Sprintf("jq transform failed: %s", e.Err)
}

func (e *TransformError) Unwrap() error {
	return e.Err
}
