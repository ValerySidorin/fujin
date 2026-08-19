# Zstandard Compression Middleware

Connector middleware that compresses message payloads using [Zstandard](https://facebook.github.io/zstd/) (zstd). Powered by [klauspost/compress](https://github.com/klauspost/compress) (pure Go).

- **Produce/HProduce:** compresses the message payload before sending to the broker.
- **Subscribe/Fetch:** decompresses the message payload before delivering to the client. If decompression fails, the message is skipped with a warning log.

Headers are not compressed — only the message body.

**Registered name:** `compress_zstd`

## Configuration

```yaml
connectors:
  my_connector:
    connector_middlewares:
      - name: compress_zstd
        level: default    # compression level (default: default)
```

| Field | Default | Description |
|-------|---------|-------------|
| `level` | `default` | Compression level: `fastest`, `default`, `better`, `best` |

## Compression Levels

| Level | Speed | Ratio | Use case |
|-------|-------|-------|----------|
| `fastest` | Highest | Lowest | High-throughput, latency-sensitive |
| `default` | Balanced | Balanced | General purpose |
| `better` | Slower | Higher | Storage-optimized |
| `best` | Slowest | Highest | Maximum compression |

## Notes

- Encoder and decoder are created once and reused — no per-message allocation overhead.
- Thread-safe: `EncodeAll` and `DecodeAll` are safe for concurrent use.
- Pair this middleware on both producer and consumer sides. Messages compressed by this middleware can only be read by a consumer with the same middleware enabled.
