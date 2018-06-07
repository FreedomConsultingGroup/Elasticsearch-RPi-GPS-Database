#!/usr/bin/python3
from requests_aws4auth import AWS4Auth
import paho.mqtt.client as mqtt
import time
import memory


def main():
    keys = open("/home/ubuntu/keys/api-keys.txt", 'r')
    usrfile = open("/home/ubuntu/keys/usrfile.pswd")
    aws_key = keys.readline().replace('\n', '')
    aws_secret = keys.readline().replace('\n', '')
    google_api_key = keys.readline().replace('\n', '')
    usrnm = usrfile.readline().replace('\n', '')
    passwd = usrfile.readline().replace('\n', '')
    usrfile.close()
    keys.close()

    region = "us-east-1"
    service = "es"
    aws_auth = AWS4Auth(aws_key, aws_secret, region, service)

    mem = memory.Memory(google_api_key, aws_auth)
    try:
        def on_connect(client, userdata, flags, rc):
            print(str(userdata))
            client.subscribe("gpsd_location")
            print("Connected with result code: " + str(rc))

        def on_message(client, userdata, msg):
            messagetime = time.time()
            payload = mem.verify(msg.payload)
            if 'error' not in payload:
                # print(payload['meta.deviceepoch'], payload['meta.type'])
                if payload["meta.type"] == "wifilocation":
                    payload["meta.messageepoch"] = messagetime
                    # print("geolocating")
                    mem.geolocate(payload)
                else:
                    # print("geocoding")
                    payload["meta.messageepoch"] = messagetime
                    mem.geocode(payload)

        client = mqtt.Client('ec2instance', clean_session=False, userdata='ec2instance')
        client.username_pw_set(usrnm, passwd)
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect('127.0.0.1', 1883, 60)

        client.loop_forever()
    finally:
        mem.stop_threads()


if __name__ == '__main__':
    main()
