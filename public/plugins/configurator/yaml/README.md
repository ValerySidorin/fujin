# YAML Configurator

Loads configuration from a YAML or JSON file.

**Registered name:** `yaml`

## Usage

```bash
export FUJIN_CONFIGURATOR=yaml
export FUJIN_CONFIGURATOR_YAML_PATHS=./config.yaml,/etc/fujin/config.yaml
```

If `FUJIN_CONFIGURATOR_YAML_PATHS` is not set, the following paths are checked in order:
- `./config.yaml`
- `conf/config.yaml`
- `config/config.yaml`

The first existing file is loaded. Both YAML and JSON formats are supported (auto-detected).
