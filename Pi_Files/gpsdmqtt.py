#!/usr/bin/python3
import paho.mqtt.client as mqtt
import time, gpsd, sys
from . import wifi
import datetime


if __name__ == '__main__':
    while True:
        log = open('/home/pi/GPSDMQTT/gpsdmqttlog.log', 'a')
        try:
            wifipoints = wifi.Wifi()
            usrfile = open("/home/pi/GPSDMQTT/usrfile.pswd")
            usrnm = usrfile.readline().replace("\n", "")
            passwd = usrfile.readline().replace("\n", "")
            log.write('----------LOG STARTED----------\n\n')


            def on_connect(client, userdata, flags, rc):
                log.write("CONNECTED::: connected with result code: " + str(rc) + '\n')

            log.write("CREATED METHOD::: on_connect \n")


            def on_message(client, userdata, msg):
                log.write("MESSAGE RECEIVED::: " + str(msg.payload) + '\n\n')

            log.write("CREATED METHOD::: on_message \n")

            client = mqtt.Client('cgood_bridge', clean_session=False, userdata='cgood_bridge')
            client.username_pw_set(usrnm, passwd)
            connection_refused = True
            log.write("CREATED CLIENT::: client \n")

            while connection_refused:
                try:
                    log.write("CONNECTING::: Attempting to connect to cgood.fcgit.net \n")
                    client.connect('cgood.fcgit.net', 1883, 60)
                    connection_refused = False
                except ConnectionRefusedError:
                    log.write("CONNECTION REFUSED ERROR::: Connection refused, retrying... \n")
                    time.sleep(1)

            log.write("MQTT CONNECTED::: Sending test message\n")
            client.publish(topic="gpsd_location", payload="{'error': 'Test publish, please ignore'}")
            log.write("CONNECTING GPSD::: Attempting to connect \n")
            client.loop_start()
            gpsd.connect()
            log.write("CONNECTING GPSD::: Connection successful. Starting Loop... \n\n")

            while True:
                try:
                    gpsdresp = gpsd.get_current()
                    devtime_epoch = time.time()
                    dt = gpsdresp.get_time()
                    log.write("COLLECTED DATA::: gps location data\n")

                    if gpsdresp.lat != 0.0 and gpsdresp.lon != 0.0:
                        payload = {"loc": {
                                       "lat": gpsdresp.lat,
                                       "lon": gpsdresp.lon
                                   },
                                   "meta.deviceepoch": devtime_epoch,
                                   "meta.type": "location",
                                   "meta.devID": "gpsd_cgood",
                                   "meta.weight": 0,
                                   "error.climb": gpsdresp.error['c'],
                                   "error.speed": gpsdresp.error['s'],
                                   "error.altitude": gpsdresp.error['v'],
                                   "error.lat": gpsdresp.error['y'],
                                   "error.lon": gpsdresp.error['x'],
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
                                   "time.second": dt.second}
                        log.write('SENT GPS MESSAGE::: Time: ' + devtime_epoch + '\n')
                        client.publish(topic='gpsd_location', payload=str(payload))
                    time.sleep(1)
                except gpsd.NoFixError or UserWarning:
                    try:
                        log.write("NO FIX ERROR::: gps might be in a bad location, or is not plugged in..\n")
                        wifiaccesspoints = wifipoints.get_cells()
                        devtime_epoch = time.time()
                        dt = datetime.datetime.utcnow()
                        payload = {
                            "wifiAccessPoints": wifiaccesspoints,
                            "meta.deviceepoch": devtime_epoch,
                            "meta.type": "wifilocation",
                            "meta.devID": "gpsd_cgood",
                            "meta.weight": 0,
                            "time.timezone": "UTC",
                            "time.year": dt.year,
                            "time.month": dt.month,
                            "time.day": dt.day,
                            "time.hour": dt.hour,
                            "time.minute": dt.minute,
                            "time.second": dt.second}
                        log.write('SENT WIFI MESSAGE::: Time: ' + devtime_epoch + '\n')
                    except Exception as e:
                        log.write("ERROR::: error getting wifi data: " + str(sys.exc_info()))
                    finally:
                        time.sleep(10)
                except ConnectionError as err:
                    connection_refused = True
                    while connection_refused:
                        try:
                            log.write("ERROR::: " + str(err) + "\n")
                            log.write("CONNECTING::: Attempting to connect to cgood.fcgit.net \n")
                            client.connect('34.197.13.189', 1883, 60)
                            connection_refused = False
                        except ConnectionRefusedError:
                            log.write("CONNECTION REFUSED ERROR::: Connection refused, retrying... \n")
                            time.sleep(1)
        except OSError:
            time.sleep(1)
        finally:
            log.close()
            # logz = open('/home/pi/GPSDMQTT/gpsdmqttlog.log_' + str(int(time.time())) + '.gz', 'w')
            # logz.write(zlib.compress(open('/home/pi/GPSDMQTT/gpsdmqttlog.log', 'r').read(), 5))
            # logz.close()
            # os.remove('/home/pi/GPSDMQTT/gpsdmqttlog.log')
