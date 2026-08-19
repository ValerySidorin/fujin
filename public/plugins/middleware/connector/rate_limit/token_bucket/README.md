# Token Bucket Rate Limit Middleware

Connector middleware that rate-limits outgoing messages using a [token bucket](https://en.wikipedia.org/wiki/Token_bucket) algorithm. Uses `golang.org/x/time/rate` (stdlib-adjacent).

- **Produce/HProduce:** if the rate limit is exceeded, the message is rejected — `callback` receives a `RateLimitError`, the message is not sent to the broker.
- **Subscribe/Fetch:** no rate limiting applied (throttling consumption is counterproductive — messages are already in the broker).

**Registered name:** `rate_limit_token_bucket`

## Configuration

```yaml
connectors:
  my_connector:
    connector_middlewares:
      - name: rate_limit_token_bucket
        rate: 1000      # requests per second
        burst: 100      # max burst size
```

| Field | Description |
|-------|-------------|
| `rate` | Maximum sustained requests per second |
| `burst` | Maximum number of requests allowed in a burst (token bucket capacity) |

Both `rate` and `burst` are required and must be positive.

## How It Works

The token bucket starts full with `burst` tokens. Each message consumes one token. Tokens are replenished at `rate` per second. If no tokens are available, the message is immediately rejected (non-blocking).

**Example:** `rate: 100, burst: 50` allows up to 50 messages instantly, then sustains 100 messages/second.
