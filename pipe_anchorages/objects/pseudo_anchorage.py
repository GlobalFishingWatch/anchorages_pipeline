from collections import namedtuple
from .namedtuples import NamedtupleCoder

PseudoAnchorage = namedtuple("PseudoAnchorage", ["mean_location", "s2id", "port_name"])


class PseudoAnchorageCoder(NamedtupleCoder):
    target = PseudoAnchorage
    time_fields = []


PseudoAnchorageCoder.register()
