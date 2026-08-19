# API Key Authentication Middleware

Bind middleware for API key authentication. Clients must pass a matching API key in the BIND request metadata.

**Registered name:** `auth_api_key`

## Configuration

```yaml
connectors:
  my_connector:
    bind_middlewares:
      - name: auth_api_key
        enabled: true
        api_key: supersecretkey
```
