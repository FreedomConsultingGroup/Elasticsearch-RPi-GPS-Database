#!/usr/bin/python3
import paho.mqtt.client as mqtt
import time, gpsd, re, subprocess


if __name__ == '__main__':
    log = open('/home/pi/Desktop/gpsdmqttlog.log', 'w')
    try:
        log.write('----------LOG STARTED----------\n\n')


        def on_connect(client, userdata, flags, rc):
            log.write("CONNECTED::: connected with result code: " + str(rc) + '\n')

        log.write("CREATED METHOD::: on_connect \n")


        def on_message(client, userdata, msg):
            log.write("MESSAGE RECEIVED::: " + str(msg.payload) + '\n\n')

        log.write("CREATED METHOD::: on_message \n")

        client = mqtt.Client('cgood_bridge', clean_session=False, userdata='cgood_bridge')
        connection_refused = True
        log.write("CREATED CLIENT::: client \n")


        while connection_refused:
            try:
                log.write("CONNECTING::: Attempting to connect to 34.197.13.189 \n")
                client.connect('34.197.13.189', 1883, 60)
                connection_refused = False
            except ConnectionRefusedError:
                log.write("CONNECTION REFUSED ERROR::: Connection refused, retrying... \n")
                time.sleep(1)

        log.write("CONNECTING GPSD::: Attempting to connect \n")
        client.loop_start()
        gpsd.connect()
        log.write("CONNECTING GPSD::: Connection successful. Starting Loop... \n\n")


        while True:
            try:
                gpsdresp = gpsd.get_current()
                devtime_epoch = time.time()
                dt = gpsdresp.get_time()
                log.write("COLLECTED DATA::: gps location data")

                if gpsdresp.lat != 0.0 and gpsdresp.lon != 0.0:
                    payload = {"meta.deviceepoch": devtime_epoch,
                               "meta.type": "location",
                               "meta.devID": "gpsd_cgood",
                               "error.climb": gpsdresp.error['c'],
                               "error.speed": gpsdresp.error['s'],
                               "error.altitude": gpsdresp.error['v'],
                               "error.lat": gpsdresp.error['y'],
                               "error.lon": gpsdresp.error['x'],
                               "pos.lat": gpsdresp.lat,
                               "pos.lon": gpsdresp.lon,
                               "pos.alt": gpsdresp.alt,
                               "pos.climb": gpsdresp.climb,
                               "pos.track": gpsdresp.track,
                               "pos.speed": gpsdresp.hspeed,
                               "time.timezone": "UTC",
                               "time.year": dt.year,
                               "time.month": dt.month,
                               "time.day": dt.day,
                               "time.hour": dt.hour,
                               "time.minute": dt.minute,
                               "time.second": dt.second,
                               "time.microsecond": dt.microsecond}
                    log.write('SENT MESSAGE::: ' + str(payload) + '\n\n')
                    client.publish(topic='gpsd_location', payload=str(payload))
                time.sleep(1)
            except UserWarning:
                time.sleep(1)
            except ConnectionError as err:
                connection_refused = True
                while connection_refused:
                    try:
                        log.write("ERROR::: " + str(err) + "\n")
                        log.write("CONNECTING::: Attempting to connect to 34.197.13.189 \n")
                        client.connect('34.197.13.189', 1883, 60)
                        connection_refused = False
                    except ConnectionRefusedError:
                        log.write("CONNECTION REFUSED ERROR::: Connection refused, retrying... \n")
                        time.sleep(1)
    finally:
        log.close()
