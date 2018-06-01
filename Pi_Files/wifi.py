import subprocess
import json
import re

def pass_braces(text):
    i = 0
    while text[i] != '{':
        i += 1
        if i == len(text):
            return 0
    i += 1
    while text[i] != '}':
        if text[i] == '{':
            i += pass_braces(text[i:]) + 1
        else:
            i += 1
    return i


def parse_json(text: str):
    i = 0
    j = 0
    interfaces = []
    while i < len(text):
        i += pass_braces(text[i:])
        if i == j:
            return interfaces
        i += 1
        interfaces.append(text[j:i])
        j = i


def separate_lines(text):
    i = j = 0
    while i < len(text):
        while text[i] != '\n':
            i += 1
        yield text[j:i]
        i += 1
        j = i


class Wifi:
    def __init__(self):
        self.iface = self.parse_iface()

    def get_cells(self):
        process = subprocess.run(['iwlist', self.iface['logicalname'], 'scan'], stdout=subprocess.PIPE)
        if process.returncode == 0:
            remac = re.compile("[a-fA-F0-9]{2}:[a-fA-F0-9]{2}:[a-fA-F0-9]{2}:[a-fA-F0-9]{2}:[a-fA-F0-9]{2}:[a-fA-F0-9]{2}")
            rechannel = re.compile("Channel:(?P<channel>\d+)")
            resignal = re.compile("Signal level=(?P<signallevel>-\d+)")
            iwlist = str(process.stdout, encoding='utf-8')
            wifiaccesspoints = []
            cell = {}
            for line in separate_lines(iwlist):
                match = remac.search(line)
                if match:
                    if 'macAddress' in cell:
                        wifiaccesspoints.append(cell)
                        cell = {}
                    cell['macAddress'] = match.group(0)
                else:
                    match = rechannel.search(line)
                    if match:
                        cell['channel'] = match.group('channel')
                    else:
                        match = resignal.search(line)
                        if match:
                            cell['signalStrength'] = match.group('signallevel')
            return wifiaccesspoints

    def parse_iface(self):
        jsondecoder = json.JSONDecoder()
        process = subprocess.run(['lshw', '-C', 'network', '-quiet', '-json'], stdout=subprocess.PIPE)
        if process.returncode == 0:
            output = str(process.stdout, encoding='utf-8')
            for interface in parse_json(output):
                iface = jsondecoder.decode(interface)
                if iface['description'] == "Wireless interface":
                    return iface
        return None
