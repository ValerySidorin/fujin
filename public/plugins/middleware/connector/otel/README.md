# OpenTelemetry Tracing Middleware

Connector middleware that provides distributed tracing via OpenTelemetry. Injects trace context into message headers and reports spans to an OTLP endpoint.

**Registered name:** `otel`

## Configuration

```yaml
connectors:
  my_connector:
    connector_middlewares:
      - name: otel
        otlp_endpoint: localhost:4317
        insecure: true
        sample_ratio: 0.1
        service_name: fujin
        service_version: "1.0.0"
        environment: production
```

| Field | Default | Description |
|-------|---------|-------------|
| `otlp_endpoint` | `localhost:4317` | OTLP gRPC collector endpoint |
| `insecure` | `true` | Disable TLS for OTLP connection |
| `sample_ratio` | `0.1` | Trace sampling ratio (0.0 to 1.0) |
| `service_name` | `fujin` | Service name in traces |
| `service_version` | `dev` | Service version in traces |
| `environment` | `dev` | Environment tag in traces |
