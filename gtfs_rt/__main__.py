import os
import json
from pprint import pformat
from venv import create
import unicodedata

from google.transit import gtfs_realtime_pb2
import requests
from dataclasses import dataclass
from dotenv import load_dotenv
import logging
import sys
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

load_dotenv()

TRANSITLAND_FEED_DIR = os.environ["TRANSITLAND_FEED_DIR"]

generate_gcmb_readme = os.environ.get("GENERATE_GCMB_README", "false") == "true"

last_modified_cache = {}

broker = os.environ.get('MQTT_HOST', 'gcmb.io')
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
class License:
    spdx_identifier: str
    use_without_attribution: bool
    create_derived_product: bool
    commercial_use_allowed: bool
    redistribution_allowed: bool
    url: str

    def can_be_used(self):
        if self.spdx_identifier in ['CC-BY-4.0', 'ODbL-1.0', 'ODC-By-1.0']:
            return True

        if self.use_without_attribution:
            return True

        if self.create_derived_product:
            return True

        if self.commercial_use_allowed:
            return True

        if self.redistribution_allowed:
            return True



@dataclass
class Feed:
    operator_name: str
    url: str
    authorization: dict
    license: License


def create_license(license_obj):
    return License(
        spdx_identifier=license_obj.get('spdx_identifier'),
        use_without_attribution=license_obj.get('use_without_attribution') == 'yes',
        create_derived_product=license_obj.get('create_derived_product') == 'yes',
        commercial_use_allowed=license_obj.get('commercial_use_allowed') == 'yes',
        redistribution_allowed=license_obj.get('redistribution_allowed') == 'yes',
        url=license_obj.get('url')
    )

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



def populate_associated_operators(operators, associated_operators):
    for operator in operators:
        associated_feeds = operator.get('associated_feeds') or []
        for associated_feed in associated_feeds:
            associated_feed_id = associated_feed.get('feed_onestop_id')
            if associated_feed_id is None:
                continue
            associated_operators[associated_feed_id] = operator



def get_rt_feeds_from_file_content(filename, data):

    feeds = []

    associated_operators = {}

    operators = data.get('operators') or []
    populate_associated_operators(operators, associated_operators)

    for feed in data['feeds']:
        spec = feed['spec']

        feed_operators = feed.get('operators') or []
        populate_associated_operators(feed_operators, associated_operators)

        if spec == 'gtfs-rt':
            urls = feed['urls']
            if not 'realtime_vehicle_positions' in urls:
                continue
            if not 'license' in feed:
                continue

            feed_license = create_license(feed['license'])
            if not feed_license.can_be_used():
                logger.info(f"Cannot use {filename} for feed {feed['id']} due to license: {feed['license']}")
                continue

            operator = feed.get('operator') or associated_operators.get(feed['id'])
            if operator is None:
                logger.info(f"No operator {filename} for feed {feed['id']}")
                continue

            authorization = feed.get('authorization')
            feeds.append(Feed(operator['name'], urls['realtime_vehicle_positions'], authorization, feed_license))

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


def get_gtfs_rt(url):

    response = requests.get(url, headers={'If-Modified-Since': last_modified_cache.get(url)})

    if response.status_code == 304:
        logger.debug("Not modified {url}")
        return None

    if not response.status_code == 200:
        return None

    last_modified = response.headers.get('Last-Modified')
    if last_modified:
        last_modified_cache[url] = last_modified

    return response.content


def get_vehicle_positions(gtfs_rt_message_pb):

    gtfs_rt_message = gtfs_realtime_pb2.FeedMessage()
    gtfs_rt_message.ParseFromString(gtfs_rt_message_pb)

    vehicle_positions = []
    for entity in gtfs_rt_message.entity:
        if entity.HasField('vehicle'):
            vehicle_positions.append(entity.vehicle)

    logger.info(f"Vehicle positions: {len(vehicle_positions)}")
    return vehicle_positions


def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])


def operator_name_to_topic(operator_name):
    return remove_accents(operator_name.replace(" ", "_").replace("/", "_").replace("-", "_").replace("(", "").replace(")", "").replace( ",", "").replace(".", "").replace(":", "").replace(";", "").replace("'", "").replace("’", ""))


def fetch_from_feed(feed: Feed, mqtt_client: mqtt.Client):

    gtfs_rt_message_pb = get_gtfs_rt(feed.url)

    if gtfs_rt_message_pb is None:
        return

    operator_topic_part = operator_name_to_topic(feed.operator_name)

    gtfs_pb_topic = f"stefan/public-transport/{operator_topic_part}/pb"
    logger.debug(f"Publishing to {gtfs_pb_topic}")

    properties = Properties(PacketTypes.PUBLISH)
    properties.UserProperty = ("Content-Type", "application/protobuf")

    # Publish protobuf gtfs-rt message representing the complete realtime state of the feed
    result = mqtt_client.publish(
        gtfs_pb_topic,
        gtfs_rt_message_pb,
        retain=True,
        properties=properties
    )
    logger.debug(f"Publish result: {result}")

    vehicle_positions = get_vehicle_positions(gtfs_rt_message_pb)

    newest_position_by_vehicle = {}
    for vehicle_position in vehicle_positions:
        vehicle_id = vehicle_position.vehicle.id
        newest_position = newest_position_by_vehicle.get(vehicle_id)
        if newest_position is None or vehicle_position.vehicle.timestamp > newest_position:
            newest_position_by_vehicle[vehicle_id] = vehicle_position.position

    # Publish individual vehicle positions
    for vehicle_id in newest_position_by_vehicle:
        position = newest_position_by_vehicle[vehicle_id]
        position_topic = f"stefan/public-transport/{operator_topic_part}/vehicle_positions/{vehicle_id}"
        payload = f"{position.latitude},{position.longitude}"
        logger.debug(f"Publishing {payload} to {position_topic}")
        result = mqtt_client.publish(position_topic, payload , retain=True)
        logger.debug(f"Publish result: {result}")


def generate_gcmb_readme(feed: Feed):
    return f"""# {feed.operator_name}
    
## License

Name: {feed.license.spdx_identifier}
URL: {feed.license.url}

## Map

<WorldMap topic="{operator_name_to_topic(feed.operator_name)}/vehicle_positions/#" />
"""


def generate_gcmb_readmes(feeds: list[Feed]):
    if generate_gcmb_readme:

        for feed in feeds:
            readme = generate_gcmb_readme(feed)
            operator_name = operator_name_to_topic(feed.operator_name)
            folder = f"gcmb/{operator_name}"
            os.makedirs(folder, exist_ok=True)
            with open(f"{folder}/README.md", "w") as f:
                f.write(readme)


def main():

    mqtt_client = connect_mqtt()
    mqtt_client.loop_start()

    rt_feeds = get_rf_feeds()
    logger.debug(f"Using the following feeds: {pformat(rt_feeds)}")

    rt_feeds_no_auth = [f for f in rt_feeds if f.authorization is None]

    generate_gcmb_readmes(rt_feeds_no_auth)

    logger.info(f"Total feeds: {len(rt_feeds)}")
    logger.info(f"Feeds without authorization: {len(rt_feeds_no_auth)}")

    # Turin
    url = 'http://percorsieorari.gtt.to.it/das_gtfsrt/vehicle_position.aspx'

    feed = [f for f in rt_feeds_no_auth if f.url == url][0]
    fetch_from_feed(feed, mqtt_client)


if __name__ == "__main__":
    main()
