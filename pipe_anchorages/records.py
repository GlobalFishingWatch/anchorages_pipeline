from collections import namedtuple
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

        # `ident` is some sort of vessel identifier, currently either `ssvid` or `vessel_id` 
        # depending if this is being used by anchorages or port_visits. Eventually, we'd
        # probably like it to be `uvi`
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
            timestamp=datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f %Z'),
            )


class VesselInfoRecord(
    namedtuple('VesselInfoRecord', ['timestamp', 'destination']),
    VesselRecord):

    __slots__ = ()

    @staticmethod
    def from_msg(msg):
        return VesselInfoRecord(
                timestamp=datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f %Z'),
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
            timestamp=datetime.datetime.strptime(msg['timestamp'], '%Y-%m-%d %H:%M:%S.%f %Z'), 
            location=latlon, 
            speed=msg['speed'],
            destination=None
           )

