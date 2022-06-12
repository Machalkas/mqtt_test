import paho.mqtt.client as mqtt
from src.clickHouseClient import ClickHouseWriter
from clickhouse_driver import Client
from datetime import datetime
import json

from src.config import CLICKHOUSE_DB_NAME, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("zigbee2mqtt/MOES Thermometer/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    data: dict = json.loads(msg.payload.decode('utf-8'))
    data["datetime"] = datetime.now()
    clickhouse.add_values(data)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set("test", "test")

clickhouse_client = Client(host=CLICKHOUSE_HOST,
                           port=CLICKHOUSE_PORT,
                           user=CLICKHOUSE_USER)
clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB_NAME}")
clickhouse = ClickHouseWriter(clickhouse_client, f"CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB_NAME}.lux (`datetime` DateTime, `battery` Nullable(Int32), `humidity` Nullable(Float32), `illuminance_lux` Nullable(Int32), `linkquality` Nullable(Int32), `temperature` Nullable(Float32), `illuminance` Nullable(Int32)) ENGINE=StripeLog()", timeout_sec=30)
client.connect("localhost", 1884, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()