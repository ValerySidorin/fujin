# Dedup Middleware

Connector middleware that deduplicates messages using an in-memory store with TTL. Prevents duplicate messages from being sent to the broker (produce) and from being delivered to the client (consume).

- **Produce/HProduce:** duplicate messages are rejected — `callback` receives a `DuplicateError`, the message is not sent to the broker.
- **Subscribe/Fetch:** duplicate messages from the broker are skipped with a debug log.

Produce and consume use separate dedup stores, so a message produced and then consumed back is not treated as a duplicate.

**Registered name:** `dedup`

## Configuration

```yaml
connectors:
  my_connector:
    connector_middlewares:
      - name: dedup
        ttl: 5m                     # how long to remember a message (default: 5m)
        cleanup_interval: 1m        # expired entry cleanup interval (default: 1m)
        key: content_hash           # dedup key strategy (default: content_hash)
```

| Field | Default | Description |
|-------|---------|-------------|
| `ttl` | `5m` | How long a message fingerprint is remembered |
| `cleanup_interval` | `1m` | How often expired entries are purged |
| `key` | `content_hash` | Key strategy: `content_hash`, `header:<name>`, or `jq:<expr>` |

## Key Strategies

### `content_hash` (default)

SHA-256 hash of the entire message payload. No configuration needed — works automatically with any message format.

```yaml
key: content_hash
```

### `header:<name>`

Deduplicates by a specific header value (e.g., an idempotency key set by the producer). For `Produce` (no headers), falls back to content hash.

```yaml
key: header:X-Message-ID
```

### `jq:<expr>`

Extracts a dedup key from the JSON payload using a jq expression. The result is hashed. Useful when messages have different timestamps but the same logical identity.

```yaml
key: jq:.event_id
```

```yaml
key: jq:[.user_id, .action]
```
