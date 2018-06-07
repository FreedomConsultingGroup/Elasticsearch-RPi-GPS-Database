#!/usr/bin/python3
import json, sys, queue, threading, time, requests
from asyncio import Lock
from elasticsearch import Elasticsearch, RequestsHttpConnection
import geohash


class Memory:
    """
    The primary memory class, keeps track of which locations have been geocoded, and which to geocode next.

             &&& &&  & &&
          && &\/&\|& ()|/ @, &&
          &\/(/&/&||/& /_/)_&/_&
       &() &\/&|()|/&\/ '%" & ()
      &_\_&&_\ |& |&&/&__%_/_& &&
    &&   && & &| &| /& & % ()& /&&
     ()&_---()&\&\|&&-&&--%---()~
         &&     \|||
                 |||
                 |||
                 |||
           , -=-~  .-^- _

     The Memory class implements a tree of base 32 digits that correspond to geohashes based on the "geohash" module.
     When geocoding information, it will only request a geocode if the user has remained within a 0.05 km radius for longer
     than 3 minutes. These conditions are set because geocoding takes longer than anything else in the process, and should
     only be done if necessary. If those conditions are satisfied, it will first search for the location within the memory.
     If the location is not stored in the memory, it will pass the payload to a queue, to be picked up by the geocoder
     thread. This thread will request the geocoded location from google maps, fill the information into the payload, then
     pass it back to be uploaded to Elasticsearch.

     The possibility also exists for road mapping, i.e. snapping location points to the closest road that the pattern follows
     in google maps.
    """
    def __init__(self, api_key, aws_auth):
        self.first = MemoryBranch()

        self.decoder = json.JSONDecoder()

        self.last_payload = None
        self.weight = 0
        self.recode = True
        # self.lock = Lock()

        self.log_queue = queue.Queue()
        self.log = Log(self.log_queue)
        self.log.start()

        self.upl_queue = queue.Queue()
        self.uploader = Uploader("Uploader", self.upl_queue, aws_auth, self.log_queue)
        self.uploader.start()

        self.geo_queue = queue.Queue()
        self.geocoder = Geocoder("Geocoder", self.geo_queue, self.upl_queue, api_key, self.log_queue)
        self.geocoder.start()

        self.glo_queue = queue.Queue()
        self.geolocator = Geolocator(self, api_key, self.glo_queue, self.log_queue)
        self.geolocator.start()

    def verify(self, msg_payload) -> dict:
        """
        The first method called by outside functions. Makes sure there are no errors in parsing JSON.

        If errors are found, it returns a dict with one key: "error", which an outer function should check for
        :param msg_payload: message received by mqtt broker
        :return: a dictionary, with either one key and value: 'error', or the full message
        """
        try:
            payload = self.decoder.decode(str(msg_payload)[2:-1].replace('\'', '\"'))
        except json.JSONDecodeError:
            try:
                payload = self.decoder.decode(str(msg_payload).replace('\'', '\"'))
            except json.JSONDecodeError:
                payload = self.decoder.decode('{\"error\": \"message not able to be parsed\"}')
        return payload

    def geolocate(self, payload: dict):
        """
        Called if the payload is of type wifilocation, tells geocoder to send geolocation data and receive a location
        in response

        :param payload: payload containing metadata and all nearby wifi points
        :return: None
        """
        self.glo_queue.put(payload)

    def geocode(self, payload: dict) -> bool:
        """
        Method called by outside functions. Highest level method of Memory class

        First checks if the user is in approximately the same location (+- 50m) as self.last_payload. If they are not,
        the old value of self.last_payload is replaces with the current payload. If they are, then it checks if they
        have been in that location for more than 3 minutes. If they have not, it moves on. If they have, it runs
        search_else_insert() on the payload.

        After that, it checks for the speed of the user. If the user's speed is > 2 km/h, it uploads the location,
        otherwise it adds 0.0167 to the weight of the next location sent, and keeps doing so until the user is once
        again moving at > 2 km/h

        :param payload: payload to geocode
        :return: True if it's geocoding, false otherwise
        """
        # print("trying for lock")
        # yield from self.lock
        # print("got lock")
        try:
            if self.last_payload is None:
                self.last_payload = payload
            geo_hash, lat_error, lon_error = geohash.geohash(payload["loc"]["lat"], payload["loc"]["lon"], 35)
            avg_error = (payload["error.lat"] + payload["error.lon"] + self.last_payload["error.lat"] + self.last_payload["error.lon"]) / 4

            # print(geohash.haversine(payload["loc"]["lat"], payload["loc"]["lon"], self.last_payload["loc"]["lat"],
            #                         self.last_payload["loc"]["lon"]))
            if geohash.haversine(payload["loc"]["lat"], payload["loc"]["lon"], self.last_payload["loc"]["lat"], self.last_payload["loc"]["lon"]) < 50 + avg_error:
                # print("that is less than " + str(50 + avg_error))
                if abs(payload["meta.deviceepoch"] - self.last_payload["meta.deviceepoch"]) > 180:
                    if self.search_else_insert(geo_hash, payload):
                        self.recode = False
                        if payload['meta.type'] == 'wifilocation':
                            self.weight += 0.167
                        else:
                            self.weight += 0.0167
                        return False
                    self.last_payload = payload
                    return True

            if payload["pos.speed"] > 2:
                self.recode = True
                # print("speed < 2")
                payload['meta.weight'] = self.weight
                self.weight = 0
                self.upl_queue.put(payload)
                self.last_payload = payload
                # print('uploaded: ' + str(payload))
            else:
                if payload['meta.type'] == 'wifilocation':
                    self.weight += 0.167
                else:
                    self.weight += 0.0167
                # print('weight is: ' + str(self.weight))
            return False
        finally:
            pass
            # print("released lock")
            # self.lock.release()

    def search_else_insert(self, geo_hash: str, payload: dict, precision: int=None):
        """
        Searches for specified geo_hash to a given precision, inserts it if it doesnt find it.

        inner method of geocode(). Only called if the user stays within a specified area for a specified amount
        of time.

        :param geo_hash: geohash to search for
        :param payload: payload to insert if geohash is not found
        :param precision: optional precision to search to. Defaults to length of geohash given
        :return: True if geohash is found to the specified precision, False otherwise
        """
        current = self.first
        level = 0
        if not precision:
            precision = len(geo_hash)

        while precision > level < len(geo_hash) and current.has_children():
            digit_not_in_children = True
            for child in current.children:
                if child.value == geo_hash[level]:
                    level += 1
                    current = child
                    digit_not_in_children = False
                    break

            if digit_not_in_children:
                self.insert(current, geo_hash[level:], payload)
                return False

        if level == precision and current.is_leaf:
            if self.recode:
                current.value = payload
                self.geo_queue.put(payload)
                return False
            return True
        self.insert(self.first, geo_hash, payload)
        return False

    def insert(self, top, geo_hash, payload):
        """
        Inner method to search_else_insert(). Abstracts the actual insertion of geohashes into the tree

        :param top: The node at which to start inserting values
        :param geo_hash: the geo_hash (or part of a geo_hash if top is not the same as self.first) to insert into the tree
        :param payload: the payload to place at the bottom of the tree
        :return: None
        """
        current = top
        self.geo_queue.put(payload)
        for digit in geo_hash:
            current = current.make_child(digit)
        current.make_child(value=payload, make_leaf=True)

    def join(self):
        """
        Calls for the geocoder thread to stop, then waits for it to do so.

        The Geocoder thread should not stop until self.queue (the same queue that the thread uses) is empty.
        :return: None
        """
        self.stop_threads()

        self.geocoder.join()

    def wait_for(self, thing):
        while thing in self.geo_queue:
            time.sleep(0.1)

    def stop_threads(self):
        """
        Waits for self.queue to empty out, then calls for the thread to stop.
        :return: None
        """
        while not self.geo_queue.empty():
            time.sleep(0.1)
        self.geocoder.__stop = True

        while not self.upl_queue.empty():
            time.sleep(0.1)
        self.uploader.__stop = True

        while not self.log_queue.empty():
            time.sleep(0.1)
        self.log.__stop = True


class MemoryNode:
    """
    ABC for MemoryBranch and MemoryLeaf classes, to make sure both are compatible with the Memory tree
    """
    def __init__(self, is_leaf: bool, value: object):
        self.value = value
        self.is_leaf = is_leaf
        self.children = None

    def make_child(self, value: object, make_leaf: bool):
        pass

    def __str__(self):
        return str(self.value)

    def has_children(self):
        if self.children and len(self.children) > 0:
            return True


class MemoryBranch(MemoryNode):
    """
    Branch in Memory tree, specifies that more values will be given to insert, able to call make_child()
    """
    def __init__(self, value=None):
        MemoryNode.__init__(self, value=value, is_leaf=False)
        self.children = []

    def make_child(self, value: object, make_leaf=False):
        if make_leaf:
            child = MemoryLeaf(value)
            self.children.append(child)
            return child
        else:
            child = MemoryBranch(value)
            self.children.append(child)
            return child


class MemoryLeaf(MemoryNode):
    """
    Leaf in Memory tree, specifies that no more values will be given to insert. self.value is the payload that the full
    geohash corresponds to
    """
    def __init__(self, value: object):
        MemoryNode.__init__(self, value=value, is_leaf=True)
        self.children = None

    def make_child(self, value: object, make_leaf=False):
        raise TypeError("Node is a leaf, and cannot have a child")


# TODO Make this a thread pool?
class Geocoder(threading.Thread):
    """
    Creates a separate thread of control for geocoding.

    Geocoding takes a large amount of time compared to everything else, so putting it in a separate thread of control
    allows it to be performed while the program runs other things.
    """
    def __init__(self, name, geo_queue, upl_queue, api_key, log_queue):
        threading.Thread.__init__(self, name=name)
        self.__stop = False
        self.geo_queue = geo_queue
        self.upl_queue = upl_queue
        self.decoder = json.JSONDecoder()
        self.api_key = api_key
        self.log_queue = log_queue
        self.daemon = True

    def run(self):
        while 1:
            try:
                if self.__stop:
                    return 0
                if self.geo_queue.empty():
                    time.sleep(0.01)
                    continue
                payload = self.geo_queue.get()
                response = requests.get("https://maps.googleapis.com/maps/api/geocode/json?latlng=" +
                                    str(payload["loc"]["lat"]) + ',' + str(payload["loc"]["lon"]) + "&key=" + self.api_key)
                location = response.json()['results'][0]

                payload["meta.type"] = "geocode"
                for dictn in location['address_components']:
                    payload['geo.'+dictn['types'][0]] = dictn['long_name']
                payload['geo.formatted_address'] = location['formatted_address']
                self.upl_queue.put(payload)
            except:
                self.log_queue.put(("Geocoder", "Error: " + str(sys.exc_info())))
                continue

    def stop_thread(self):
        """
        Should not be called unless self.queue is empty
        :return: None
        """
        self.__stop = True


class Geolocator(threading.Thread):
    def __init__(self, memory, api_key, glo_queue: queue.Queue, log_queue: queue.Queue):
        threading.Thread.__init__(self, name="Geolocator")
        self.memory = memory
        self.glo_queue = glo_queue
        self.log_queue = log_queue
        self.api_key = api_key
        self.__stop = False
        self.last_payload = None
        self.daemon = True

    def run(self):
        while 1:
            try:
                if self.__stop:
                    return 0
                elif self.glo_queue.empty():
                    time.sleep(1)
                    continue
                # print("something in queue")
                payload = self.glo_queue.get()
                jsonpayload = {"wifiAccessPoints": payload["wifiAccessPoints"], }
                response = requests.post(url="https://www.googleapis.com/geolocation/v1/geolocate?key=" + self.api_key,
                                         json=jsonpayload)
                if response.status_code == 200:
                    responsejson = response.json()
                    location = responsejson['location']
                    error = responsejson['accuracy']
                    del payload["wifiAccessPoints"]
                    payload['loc'] = {'lat': location['lat'], 'lon': location['lng']}
                    payload['error.lat'] = error
                    payload['error.lon'] = error
                    if self.last_payload is None:
                        payload['pos.speed'] = 0
                    else:
                        payload['pos.speed'] = geohash.haversine(location['lat'], location['lng'], self.last_payload['loc']['lat'], self.last_payload['loc']['lon']) / (payload['meta.deviceepoch'] - self.last_payload['meta.deviceepoch'])
                    # print(location['lat'], location['lng'])
                    self.memory.geocode(payload)
                    # print(payload)
                    self.last_payload = payload
                else:
                    response.raise_for_status()
            except:
                self.log_queue.put(("Geolocator", "Error: " + str(sys.exc_info())))
                continue

    def stop_thread(self):
        """
        Should not be called unless self.queue is empty
        :return: None
        """
        self.__stop = True


class Uploader(threading.Thread):
    def __init__(self, name, upl_queue, aws_auth, log_queue):
        threading.Thread.__init__(self, name=name)
        self.__stop = False

        endpoint = "search-chriswillelasticsearch-sbzs5dhk3efss3t4bidlxmym7u.us-east-1.es.amazonaws.com"
        self.esnode = Elasticsearch(
            hosts=[{'host': endpoint, 'port': 443}],
            http_auth=aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
        self.upl_queue = upl_queue
        self.log_queue = log_queue
        self.daemon = True

    def run(self):
        while 1:
            try:
                if self.__stop:
                    return 0
                if self.upl_queue.empty():
                    time.sleep(1)
                    continue

                payload = self.upl_queue.get()
                if payload["meta.type"] == "geocode":
                    self.upload_location(payload)
                    self.log_queue.put(("Uploader", "sent geocoded payload\n"))
                else:
                    self.upload_location(payload)
            except:
                self.log_queue.put(("Uploader", "Error: " + str(sys.exc_info()) + "\n"))

    def upload_location(self, payload):
        self.esnode.index(index=payload["meta.devID"], doc_type="location_data", body=payload)

    def stop_thread(self):
        """
        Should not be called unless self.queue is empty
        :return: None
        """
        self.__stop = True


class Log(threading.Thread):
    """
    Separate thread of control for logging, to allow all running threads to log data.

    Each payload in the queue should be a tuple where index 0 is the name of the thread the message is from and index 1
    is the message to be logged
    """
    def __init__(self, log_queue):
        threading.Thread.__init__(self, name="Logging")
        self.log = open("/home/ubuntu/FILES/mqtt-es/mqtt-es.log", "a")
        self.log_queue = log_queue
        self.__stop = False
        self.daemon = True

    def run(self):
        while 1:
            try:
                if self.__stop:
                    return 0
                if self.log_queue.empty():
                    time.sleep(0.1)

                payload = self.log_queue.get()
                self.log.write(str(time.time()) + " - " + payload[0] + ":   " + payload[1] + '\n')
            except:
                print("error")
                time.sleep(0.5)
                continue

    def stop_thread(self):
        """
        Should not be called unless self.queue is empty
        :return: None
        """
        self.__stop = True
