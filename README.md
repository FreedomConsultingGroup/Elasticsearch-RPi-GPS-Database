# Elasticsearch-RPi-GPS-Database

This repo holds files for transfering GPS data from a raspberry pi to an EC2 instance on AWS via mqtt, then uploading that data to an Elasticsearch database.

Dependencies:
1. EC2 Instance: 
   * APT
   * * [Mosquitto](https://mosquitto.org/ "Mosquitto")
   * * [elasticsearch](https://pypi.python.org/pypi/elasticsearch/2.3.0 "Python Package Index: elasticsearch")
   * PIP
   * * [requests-aws4auth](https://pypi.python.org/pypi/requests-aws4auth "Python Package Index: requests-aws4auth")
   * * [paho-mqtt](https://pypi.python.org/pypi/paho-mqtt/1.3.1 "Python Package Index: paho-mqtt")
2. Raspberry Pi: 
   * APT
   * * [Mosquitto](https://mosquitto.org/ "Mosquitto")
   * PIP
   * * [paho-mqtt](https://pypi.python.org/pypi/paho-mqtt/1.3.1 "Python Package Index: paho-mqtt")
   * * [gpsd-py3](https://pypi.python.org/pypi/gpsd-py3/0.2.0 "Python Package Index: gpsd-py3")

## How-To


