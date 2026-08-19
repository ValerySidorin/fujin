# JSON Schema Validation Middleware

Connector middleware that validates message payloads against a JSON Schema (supports up to draft 2020-12).

- **Produce/HProduce:** invalid messages are rejected — `callback` receives a `ValidationError`, the message is not sent to the broker.
- **Subscribe/Fetch:** invalid messages from the broker are skipped with a warning log.

**Registered name:** `schema_json`

## Configuration

```yaml
connectors:
  my_connector:
    connector_middlewares:
      - name: schema_json
        # Inline JSON Schema:
        schema: '{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}'

        # Or load from file:
        # schema_path: /path/to/schema.json
```

| Field | Description |
|-------|-------------|
| `schema` | Inline JSON Schema string |
| `schema_path` | Path to a JSON Schema file |

One of `schema` or `schema_path` is required.
