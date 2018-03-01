#!/usr/bin/python3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import paho.mqtt.client as mqtt
import time, memory


def main():
    keys = open("/home/ubuntu/chrisStuff/keys.txt", 'r')
    aws_key = keys.readline().replace('\n', '')
    aws_secret = keys.readline().replace('\n', '')
    googleapikey = keys.readline().replace('\n', '')
    keys.close()



    region = "us-east-1"
    service = "es"
    aws_auth = AWS4Auth(aws_key, aws_secret, region, service)

    endpoint = "search-chriswillelasticsearch-sbzs5dhk3efss3t4bidlxmym7u.us-east-1.es.amazonaws.com"
    esnode = Elasticsearch(
        hosts = [{'host': endpoint, 'port': 443}],
        http_auth = aws_auth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )

    def on_connect(client, userdata, flags, rc):
        print(str(userdata))
        client.subscribe("gpsd_location")
        print("Connected with result code: " + str(rc))

    def on_message(client, userdata, msg):
        messagetime = int(round(time.time() * 1000))
        global mem
        mem = memory.Memory(googleapikey)
        payload = mem.verify(msg)
        if 'error' not in payload:
            if mem.geocode(payload):
                mem.wait_for(payload)
                esnode.index(index=payload["meta.devID"], doc_type="geocode_data", body=payload)
            else:
                esnode.index(index=payload["meta.devID"], doc_type="location_data", body=payload)

        print(str(payload))



    client = mqtt.Client('ec2instance', clean_session=False, userdata='ec2instance')
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect('127.0.0.1', 1883, 60)

    client.loop_forever()


if __name__ == '__main__':
    main()
