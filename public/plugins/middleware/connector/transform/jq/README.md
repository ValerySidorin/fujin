# jq Transform Middleware

Connector middleware that transforms message payloads using [jq](https://jqlang.github.io/jq/) expressions. Powered by [gojq](https://github.com/itchyny/gojq) (pure Go, no CGO).

- **Produce/HProduce:** transforms the message before sending to the broker. If the transform fails, the message is rejected — `callback` receives a `TransformError`.
- **Subscribe/Fetch:** transforms messages from the broker before delivering to the client. If the transform fails, the message is skipped with a warning log.

**Registered name:** `transform_jq`

## Configuration

```yaml
connectors:
  my_connector:
    connector_middlewares:
      - name: transform_jq
        # jq expression for outgoing messages (produce):
        produce: '{temperature: .t, humidity: .h, device_id: .dev, unit: "celsius"}'

        # jq expression for incoming messages (consume):
        consume: '{userName: .user_name, age: .age}'
```

| Field | Description |
|-------|-------------|
| `produce` | jq expression applied to outgoing messages (optional) |
| `consume` | jq expression applied to incoming messages (optional) |

At least one of `produce` or `consume` is required. If only one is specified, the other direction passes messages through unchanged.

## Examples

Rename fields before sending to broker:
```yaml
produce: '{temperature: .t, humidity: .h}'
```

Add a static field:
```yaml
produce: '. + {source: "sensor-gateway"}'
```

Extract nested data:
```yaml
consume: '{id: .metadata.id, value: .payload.value}'
```

Filter fields:
```yaml
consume: '{name, email}'
```
