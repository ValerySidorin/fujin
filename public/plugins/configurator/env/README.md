# Environment Variable Configurator

Loads configuration from the `FUJIN_CONFIGURATOR_ENV_CONFIG` environment variable. Supports both YAML and JSON formats (auto-detected).

**Registered name:** `env`

## Usage

```bash
export FUJIN_CONFIGURATOR=env
export FUJIN_CONFIGURATOR_ENV_CONFIG='{"fujin":{"transports":[{"type":"tcp","settings":{"addr":":4850"}}]},"grpc":{"enabled":false}}'
./bin/fujin
```

Or with YAML:

```bash
export FUJIN_CONFIGURATOR=env
export FUJIN_CONFIGURATOR_ENV_CONFIG='
fujin:
  transports:
    - type: tcp
      settings:
        addr: ":4850"
grpc:
  enabled: false
connectors:
  my_connector:
    type: kafka_franz
    settings:
      common:
        brokers: ["localhost:9092"]
'
./bin/fujin
```

## Kubernetes

ConfigMap as env var:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fujin-config
data:
  config: |
    fujin:
      transports:
        - type: tcp
          settings:
            addr: ":4850"
    connectors:
      my_connector:
        type: kafka_franz
---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: fujin
          env:
            - name: FUJIN_CONFIGURATOR
              value: env
            - name: FUJIN_CONFIGURATOR_ENV_CONFIG
              valueFrom:
                configMapKeyRef:
                  name: fujin-config
                  key: config
```
