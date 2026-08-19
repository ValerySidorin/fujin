package json

import (
	stdjson "encoding/json"
	"fmt"
	"log/slog"
	"strings"

	jschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/fujin-io/fujin/public/plugins/connector"
	cmw "github.com/fujin-io/fujin/public/plugins/middleware/connector"
)

func init() {
	if err := cmw.Register("schema_json", newSchemaMiddleware); err != nil {
		panic(fmt.Sprintf("register schema_json connector middleware: %v", err))
	}
}

type schemaMiddleware struct {
	schema *jschema.Schema
	l      *slog.Logger
}

func newSchemaMiddleware(config any, l *slog.Logger) (cmw.Middleware, error) {
	var schemaPath, schemaInline string

	if m, ok := config.(map[string]any); ok {
		if v, ok := m["schema_path"].(string); ok {
			schemaPath = v
		}
		if v, ok := m["schema"].(string); ok {
			schemaInline = v
		}
	}

	if schemaPath == "" && schemaInline == "" {
		return nil, fmt.Errorf("schema middleware: one of 'schema_path' or 'schema' is required")
	}

	compiled, err := compileSchema(schemaPath, schemaInline)
	if err != nil {
		return nil, fmt.Errorf("schema middleware: compile schema: %w", err)
	}

	l.Info("schema validation middleware initialized")
	return &schemaMiddleware{schema: compiled, l: l}, nil
}

func compileSchema(path, inline string) (*jschema.Schema, error) {
	c := jschema.NewCompiler()

	if path != "" {
		return c.Compile(path)
	}

	// Inline schema: add to compiler as a resource, then compile.
	var schemaDoc any
	if err := stdjson.Unmarshal([]byte(inline), &schemaDoc); err != nil {
		return nil, fmt.Errorf("parse inline schema JSON: %w", err)
	}
	if err := c.AddResource("inline.json", schemaDoc); err != nil {
		return nil, fmt.Errorf("add inline schema resource: %w", err)
	}
	return c.Compile("inline.json")
}

func (m *schemaMiddleware) validate(msg []byte) error {
	var v any
	if err := stdjson.Unmarshal(msg, &v); err != nil {
		return fmt.Errorf("message is not valid JSON: %w", err)
	}
	err := m.schema.Validate(v)
	if err != nil {
		return &ValidationError{Err: err}
	}
	return nil
}

// ValidationError wraps a schema validation failure.
type ValidationError struct {
	Err error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("schema validation failed: %s", e.Err)
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

func (m *schemaMiddleware) WrapWriter(w connector.WriteCloser, connectorName string) connector.WriteCloser {
	return &schemaWriterWrapper{
		w:  w,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

func (m *schemaMiddleware) WrapReader(r connector.ReadCloser, connectorName string) connector.ReadCloser {
	return &schemaReaderWrapper{
		r:  r,
		mw: m,
		l:  m.l.With("connector", connectorName),
	}
}

// truncate returns first n bytes of s for log messages.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// formatValidationError formats the error for logging, trimming verbose output.
func formatValidationError(err error) string {
	s := err.Error()
	// Limit to first line for log readability.
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return s[:i]
	}
	return truncate(s, 200)
}
