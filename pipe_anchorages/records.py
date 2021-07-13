from collections import namedtuple
from .objects.namedtuples import s_to_datetime
import datetime

def is_location_message(msg):
    return (
        msg['lat'] is not None and
        msg['lon'] is not None and
        msg['speed'] is not None
        )

def has_valid_location(msg):
    return (
        -90  <= msg['lat']   <= 90  and
        -180 <= msg['lon']   <= 180 and
        0    <= msg['speed'] <= 102.2
    )

def has_destination(msg):
    return msg['destination'] not in ('', None)


class VesselRecord(object):

    @staticmethod
    def tagged_from_msg(msg):

        # `ident` is some sort of vessel identifier, currently either `ssvid`, `seg_id`, 'vessel_id' or 'track_id'  
        ident = msg['ident']

        if is_location_message(msg) and has_valid_location(msg):
            return (ident, VesselLocationRecord.from_msg(msg))
        elif has_destination(msg):
            return (ident, VesselInfoRecord.from_msg(msg))
        else:
            return (ident, InvalidRecord.from_msg(msg))



class InvalidRecord(
    namedtuple('InvalidRecord', ['timestamp']),
    VesselRecord):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        return InvalidRecord(
            timestamp=s_to_datetime(msg['timestamp']),
            )


class VesselInfoRecord(
    namedtuple('VesselInfoRecord', ['timestamp', 'destination']),
    VesselRecord):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        timestamp=None
        try:
            timestamp=datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f %Z')
        except ValueError:
            timestamp=datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S %Z')
        return VesselInfoRecord(
                timestamp=s_to_datetime(msg['timestamp']),
                destination=msg['destination']
                )


class VesselLocationRecord(
    namedtuple("VesselLocationRecord", ['timestamp', 'location', 'speed', 'destination']),
    VesselRecord):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        from .common import LatLon
        latlon = LatLon(msg['lat'], msg['lon'])
        return VesselLocationRecord(
            timestamp=s_to_datetime(msg['timestamp']),
            location=latlon,
            speed=msg['speed'],
            destination=None
           )

