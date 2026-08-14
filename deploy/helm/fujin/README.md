# Fujin Helm Chart

## Modes

### Standalone (default)

Fujin runs as a separate Deployment with its own Service.

```bash
helm install fujin ./deploy/helm/fujin
```

### Sidecar

Generates a ConfigMap for Fujin config. Add the sidecar container to your own Deployment using the rendered template as a reference, or include the chart as a subchart.

```bash
helm install fujin ./deploy/helm/fujin --set mode=sidecar
```

## Values

See [values.yaml](values.yaml) for all configuration options.

The chart defaults to the immutable Fujin image tag matching `appVersion`. Override `image.repository` or `image.tag` when publishing through a different registry.
