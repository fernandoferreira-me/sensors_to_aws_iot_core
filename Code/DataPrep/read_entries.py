from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder

import re
import time as t
import json

ENDPOINT = "a3szn5rpqfw28x-ats.iot.us-east-1.amazonaws.com"
CLIEND_ID = "computer"
PATH_TO_CERTIFICATE = "/app/Code/DataPrep/certificates/computer.cert.pem"
PATH_TO_PRIVATE_KEY = "/app/Code/DataPrep/certificates/computer.private.key"
PATH_TO_AMAZON_ROOT_CA_1 = "/app/Code/DataPrep/certificates/root-CA.crt"

TOPIC = "computer/temperature"


def read_last_entries(current_line=0):
    with open("./Data/Raw/sensors.txt", "r") as entries:
        entries.seek(current_line)
        for line in entries:
            yield line

def filter_content(line, pattern="CPU die temperature:"):
    if re.search(pattern, line):
        value = re.search(r"\d+\.\d+", line).group(0)
        return float(value)
    return None


if __name__ == "__main__":
    read_from_line = 0
    while True:
        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group,  host_resolver)
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=PATH_TO_CERTIFICATE,
            pri_key_filepath=PATH_TO_PRIVATE_KEY,
            ca_filepath=PATH_TO_AMAZON_ROOT_CA_1,
            client_bootstrap=client_bootstrap,
            client_id=CLIEND_ID,
            clean_session=False,
            keep_alive_secs=6
        )
        connection_future = mqtt_connection.connect()
        connection_future.result()
        for idx, line in enumerate(read_last_entries(current_line=read_from_line)):
            content = filter_content(line)
            if content:
                message = {"temperature" :  content}
                print("Sending message to Aws Iot Core: {}".format(content))
                mqtt_connection.publish(topic=TOPIC,
                                                            payload=json.dumps(message),
                                                            qos=mqtt.QoS.AT_LEAST_ONCE)
                t.sleep(.1)
        read_from_line = idx
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        t.sleep(4)
