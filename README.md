AlphaVantageMQTT
================
Polls Alpha Vantage servers for supplied stocks and publishes to a mqtt broker.

## Configuration
All configuration is done via the `config.env` file.

### Example
```
[SECRET]
api_key: 1234567890abc

[CONFIG]
interval: 3600
timeout: 0.005
trace: False
tickers: goog, fdx, amzn

[MQTT]
broker: localhost
port: 1883
username: mqtt_user
password: mqtt
root_topic: stocks/

```