import os
import json
from google.transit import gtfs_realtime_pb2
import requests
from dataclasses import dataclass
from dotenv import load_dotenv
import logging
import sys
import paho.mqtt.client as mqtt

load_dotenv()

feed = gtfs_realtime_pb2.FeedMessage()

TRANSITLAND_FEED_DIR = os.environ["TRANSITLAND_FEED_DIR"]

last_modified_cache = {}

broker = 'gcmb.io'
port = 8883
client_id = 'stefan/public-transport/data-publisher/pub'
username = os.environ['MQTT_USERNAME']
password = os.environ['MQTT_PASSWORD']

log_level = os.environ.get('LOG_LEVEL', 'INFO')
print("Using log level", log_level)

logger = logging.getLogger()
logger.setLevel(log_level)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


@dataclass
class Feed:
    operator_name: str
    url: str
    authorization: dict


def connect_mqtt():
    def on_connect(client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("Connected to MQTT Broker")
        else:
            logger.error(f"Failed to connect, return code {rc}")

    mqtt_client = mqtt.Client(client_id=client_id,
                              callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.tls_set(ca_certs='/etc/ssl/certs/ca-certificates.crt')
    mqtt_client.username_pw_set(username, password)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = lambda client, userdata, disconnect_flags, reason_code, properties: logger.warning(
        f"Disconnected from MQTT Broker, return code {reason_code}")
    mqtt_client.connect(broker, port)
    return mqtt_client


def can_be_used(license_obj):
    if license_obj.get('spdx_identifier') in ['CC-BY-4.0', 'ODbL-1.0', 'ODC-By-1.0']:
        return True

    if license_obj.get('use_without_attribution') == 'yes':
        return True

    if license_obj.get('create_derived_product') == 'yes':
        return True

    if license_obj.get('commercial_use_allowed') == 'yes':
        return True

    if license_obj.get('redistribution_allowed') == 'yes':
        return True


def get_rt_feeds_from_file_content(filename, data):

    # pprint.pp(data)
    feeds = []

    associated_operators = {}

    operators = data.get('operators') or []

    for operator in operators:
        associated_feeds = operator.get('associated_feeds') or []
        for associated_feed in associated_feeds:
            associated_feed_id = associated_feed.get('feed_onestop_id')
            if associated_feed_id is None:
                continue
            associated_operators[associated_feed_id] = operator

    for feed in data['feeds']:
        spec = feed['spec']

        feed_operators = feed.get('operators') or []
        for operator in feed_operators:
            associated_feeds = operator.get('associated_feeds') or []
            for associated_feed in associated_feeds:
                ssociated_feed_id = associated_feed.get('feed_onestop_id')
                if associated_feed_id is None:
                    continue
                associated_operators[associated_feed_id] = operator

        if spec == 'gtfs-rt':
            urls = feed['urls']
            if not 'realtime_vehicle_positions' in urls:
                continue
            if not 'license' in feed:
                continue

            license_obj = feed['license']
            if not can_be_used(license_obj):
                print("Cannot use", filename, "for feed", feed['id'], "due to license:", license_obj)
                continue

            operator = feed.get('operator') or associated_operators.get(feed['id'])
            if operator is None:
                print("No operator", filename, "for feed", feed['id'])
         mqtt_client = connect_mqtt()       continue

            authorization = feed.get('authorization')
            feeds.append(Feed(operator['name'], urls['realtime_vehicle_positions'], authorization))

    return feeds


def get_rt_feeds_from_file(filename):
    with open(filename, "r") as f:
        content = f.read()
        data = json.loads(content)
        return get_rt_feeds_from_file_content(filename, data)


def get_rf_feeds():

    all_feeds = []

    for file in os.listdir(TRANSITLAND_FEED_DIR):
        if not file.endswith(".json"):
            continue

        feeds = get_rt_feeds_from_file(TRANSITLAND_FEED_DIR + '/' + file)
        all_feeds += feeds

    return all_feeds


def get_vehicle_positions(url):

    response = requests.get(url, headers={'If-Modified-Since': last_modified_cache.get(url)})

    if response.status_code == 304:
        print("Not modified", url)
        return []

    if not response.status_code == 200:
        return []

    last_modified = response.headers.get('Last-Modified')
    if last_modified:
        last_modified_cache[url] = last_modified

    feed.ParseFromString(response.content)
    vehicle_positions = []
    for entity in feed.entity:
        # print(entity)
        if entity.HasField('vehicle'):
            print(entity.vehicle)
            vehicle_positions.append(entity.vehicle)

    return vehicle_positions


def operator_name_to_topic(operator_name):
    return operator_name.replace(" ", "_").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "").replace( ",", "").replace(".", "").replace(":", "").replace(";", "").replace("'", "").replace("’", "")

def main():

    rt_feeds = get_rf_feeds()

    # rt_feeds = get_rt_feeds_from_file("exo.quebec.dmfr.json")
    # pprint.pp(rt_feeds)

    rt_feeds_no_auth = [f for f in rt_feeds if f.authorization is None]
    
    print("Total feeds:", len(rt_feeds))
    print("Feeds without authorization:", len(rt_feeds_no_auth))

    # Germany, but does not have vehicle positions
    # url = 'https://realtime.gtfs.de/realtime-free.pb'

    # Turin
    url = 'http://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx'
    vehicle_positions = get_vehicle_positions(url)

    feed = [f for f in rt_feeds_no_auth if f.url == url][0]
    print(operator_name_to_topic(feed.operator_name))

    for vehicle_position in vehicle_positions:
        topic = f"stefan/gtfs-rt/{operator_name_to_topic(feed.operator_name)}/vehicle_positions/{vehicle_position.vehicle.id}"
        print(f"Publishing to {topic}")

    # Publish to MQTT:
    # stefan/gtfs-rt/GTT_Servizio_Ferroviario/vehicle_positions/9060 -> Lat,Lon
    # stefan/gtfs-rt/GTT_Servizio_Ferroviario/pb -> Protobuf message


    # for feed in rt_feeds_no_auth:
    #     print(feed)
    #     try:
    #         get_vehicle_positions(feed.url)
    #     except Exception as e:
    #         print("Error", feed, e)


if __name__ == "__main__":
    main()
