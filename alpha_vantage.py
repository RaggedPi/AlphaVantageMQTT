#!/usr/bin/env python

################################################################
# Imports                                                      #
################################################################
# Globals
import argparse
import json
import time
import uuid
import logging
from collections import OrderedDict
from datetime import datetime
from tzlocal import get_localzone
from alpha_vantage.timeseries import TimeSeries
from concurrent.futures.thread import ThreadPoolExecutor
import paho.mqtt.client as mqtt
import configparser

################################################################
# Variables                                                    #
################################################################
# UUID
uuidstr = str(uuid.uuid1())
# Parsers
parser = argparse.ArgumentParser(description="AlphaVantage MQTT Publisher")
config = configparser.ConfigParser()
# Parser Groups
logger = parser.add_argument_group("MQTT publish")
# Logging
log = logging.getLogger(__name__)
file_hander = logging.FileHandler('alpha_vantage.log')
file_hander.setFormatter(
    logging.Formatter('%(asctime)s :: %(loglevel)s :: %(message)s'))
log.addHandler(file_hander)


################################################################
# Methods                                                      #
################################################################
# Get Current Stock Prices
def publish(symbol, price):
    # Connect
    try:
        client.connect(
            config.get("MQTT", "broker"))
        client.loop_start()
    except Exception as e:
        log.error("Failed to connect to broker. {}".format(e))

    # Build Topic
    topic = "{}{}".format(config.get("MQTT", "root_topic"), symbol)

    # Build Payload
    data = OrderedDict()
    data["datetime"] = datetime.now(
        get_localzone()).replace(microsecond=0).isoformat()

    data["symbol"] = symbol
    data["price"] = price

    payload = json.dumps(
      data,
      indent=None,
      ensure_ascii=True,
      allow_nan=True,
      separators=(',', ':'))

    # Publish To Broker
    try:
        client.publish(topic, payload=payload)
    except Exception as e:
        log.error("Failed to publish payload to broker. {}".format(e))

    # Disconnect
    client.disconnect()
    client.loop_stop()


# Get a Config list
def getConfigList(option, sep=',', chars=None):
    """Return a list from a ConfigParser option. By default,
       split on a comma and strip whitespaces."""
    return [chunk.strip(chars) for chunk in option.split(sep)]


################################################################
# Callbacks                                                    #
################################################################
# OnConnect
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag = True
        log.debug("Connected. [RC: {}]".format(rc))
    else:
        client.bad_connection_flag = True
        log.warning("Bad connection. [RC: {}]".format(rc))


# OnLog
def on_log(client, obj, mid):
    log.debug("Mid: {}".format(str(mid)))


# OnDisconnect
def on_disconnect(client, userdata, rc):
    log.debug("Disconnected. ReasonCode={}".format(rc))


# OnPublish
def on_publish(client, obj, mid):
    log.debug("Mid: {}".format(str(mid)))


################################################################
# Parse Config                                                 #
################################################################
# Read config file
config.read('config.env')

# Check interval
if config.getint("CONFIG", "interval") < 10 or config.getint("CONFIG", "interval") > (60*60):
    log.error(
        "argument -i/--interval: must be between 10 seconds and 3600 (1 hour)")

# Ensure Proper Topic Formatting
if config["MQTT"]["root_topic"][-1] != "/":
    config["MQTT"]["root_topic"] += "/"

# Log Configured Options
log.debug(
     "Options:{}".format(
          str(config["CONFIG"]).replace("Namespace(", "").replace(")", "")))

# Notify
log.info("Publishing to broker:{} Every:{} seconds".format(
    config.get("MQTT", "BROKER"), config.get("CONFIG", "interval")))

################################################################
# MQTT Configuration                                           #
################################################################
# Set MQTT Flags
mqtt.Client.connected_flag = False
mqtt.Client.bad_connection_flag = False
# MQTT Client
client = mqtt.Client(
    client_id=uuidstr,
    clean_session=False)
client.username_pw_set(
    username=config.get("MQTT", "username"),
    password=config.get("MQTT", "password"))

################################################################
# Set Callbacks                                                #
################################################################
client.on_connect = on_connect
client.on_log = on_log
client.on_disconnect = on_disconnect
client.on_publish = on_publish

################################################################
# Main Loop                                                    #
################################################################
while True:
    # Start Time
    start = time.time()

    try:
        # Poll for data
        tickers = getConfigList(config.get("CONFIG", "tickers"))
        ts = TimeSeries(key=config.get("SECRET", "api_key"))

        with ThreadPoolExecutor() as executer:
            data = executer.map(
                      lambda stock:ts.get_quote_endpoint(symbol=stock),
                      tickers)
        for item in data:
            publish(item[0]["01. symbol"], item[0]["05. price"])

    except Exception as e:
        log.error(
            "Error polling data: {}".format(e))

    # Calculate Sleep Timer
    interval = time.time() - start
    sleep = config.getint("CONFIG", "interval") - interval
    if sleep > 0:
        time.sleep(sleep)
