# MQTT Connector (Paho)

MQTT connector using Eclipse Paho. Works with any MQTT broker (EMQX, Mosquitto, NanoMQ, etc.).

**Registered name:** `mqtt_paho`

## Configuration

```yaml
connectors:
  my_mqtt:
    type: mqtt_paho
    settings:
      common:
        broker_url: tcp://localhost:1883
        keep_alive: 30
        connect_timeout: 10s
        disconnect_timeout: 5s
      routes:
        pub:
          client_id: fujin_pub
          topic: my/topic
          qos: 1
          retain: false
          clean_start: true
        sub:
          client_id: fujin_sub
          topic: my/topic
          qos: 1
          clean_start: true
          session_expiry: 3600
```
