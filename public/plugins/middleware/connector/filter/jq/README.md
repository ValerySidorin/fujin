# jq Filter Middleware

Connector middleware that filters messages using [jq](https://jqlang.github.io/jq/) expressions. The expression must return a boolean — `true` passes the message through, `false` or `null` drops it. Powered by [gojq](https://github.com/itchyny/gojq) (pure Go, no CGO).

- **Produce/HProduce:** if the filter returns false, the message is rejected — `callback` receives a `FilteredError`, the message is not sent to the broker.
- **Subscribe/Fetch:** if the filter returns false, the message is skipped with a debug log.

Follows [jq truthiness](https://jqlang.github.io/jq/manual/#type-system): only `false` and `null` are falsy. Everything else (including `0`, `""`, `[]`, `{}`) is truthy.

**Registered name:** `filter_jq`

## Configuration

```yaml
connectors:
  my_connector:
    connector_middlewares:
      - name: filter_jq
        # jq expression for outgoing messages (produce):
        produce: '.priority == "high" or .retry_count < 3'

        # jq expression for incoming messages (consume):
        consume: '.country == "US"'
```

| Field | Description |
|-------|-------------|
| `produce` | jq expression applied to outgoing messages, must return boolean (optional) |
| `consume` | jq expression applied to incoming messages, must return boolean (optional) |

At least one of `produce` or `consume` is required. If only one is specified, the other direction passes messages through unchanged.

## Examples

Only send high-priority events:
```yaml
produce: '.priority == "high"'
```

Drop messages older than 1 hour (assumes `.timestamp` is Unix seconds):
```yaml
consume: '(now - .timestamp) < 3600'
```

Only deliver messages with specific fields present:
```yaml
consume: '.user_id != null and .action != null'
```

Filter by array content:
```yaml
produce: '.tags | any(. == "important")'
```
