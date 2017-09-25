from collections import namedtuple
import datetime

def is_location_message(msg):
    return (
        msg.get('lat') is not None and 
        msg.get('lon') is not None
        )

def has_valid_location(msg):
    return (
        -90  <= msg['lat'] <= 90 and
        -180 <= msg['lon'] <= 180
    )

def has_destination(msg):
    return msg['destination'] not in ('', None)


class VesselRecord(object):

    @staticmethod
    def from_msg(msg):

        mmsi = msg.get('mmsi')

        if not isinstance(mmsi, int):
            return None

        if is_location_message(msg) and has_valid_location(msg):
            return (mmsi, VesselLocationRecord.from_msg(msg))
        elif has_destination(msg):
            return (mmsi, VesselInfoRecord.from_msg(msg))
        else:
            return None


class VesselInfoRecord(
    namedtuple('VesselInfoRecord', ['timestamp', 'destination']),
    VesselRecord):

    @staticmethod
    def from_msg(msg):
        return VesselInfoRecord(
                datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f %Z'),
                msg['destination']
                )


class VesselLocationRecord(
    namedtuple("VesselLocationRecord", ['timestamp', 'location', 'destination']),
    VesselRecord):

    @staticmethod
    def from_msg(msg):
        from .common import LatLon
        latlon = LatLon(msg['lat'], msg['lon'])

        return VesselLocationRecord(
            datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f %Z'), 
            latlon, 
            None
           )

