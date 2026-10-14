"""Builders for the records these transforms work on."""

from pipe_anchorages.core.named_anchorage_point import NamedAnchoragePoint

KM_PER_DEGREE_LAT = 111.2

# An anchorage of anchorage_overrides.csv with a sublabel, and no other place within 6 km.
POINTE_NOIRE_S2ID = "1a5e4eaf"
POINTE_NOIRE_LAT, POINTE_NOIRE_LON = -5.377980626534826, 11.442074157998116

# Far from any place of the reference lists, in no country's EEZ.
HIGH_SEAS_LAT, HIGH_SEAS_LON = -30.0, -120.0

# Far from any place of the reference lists, in China's EEZ.
EAST_CHINA_SEA_LAT, EAST_CHINA_SEA_LON = 30.0, 124.0

# The anchorage the overrides remove.
REMOVED_S2ID = "10000001"

# anchorage_overrides.csv has 60410 anchorages.
N_OVERRIDES = 60410

PORT_INFO_CONFIG = dict(
    override_path="anchorage_overrides.csv",
    port_list_paths=["peru.csv", "indonesia.csv", "WPI_ports.csv", "geonames_1000.csv"],
    label_distance_km=4.0,
    sublabel_distance_km=1.0,
)


def anchorage_location(s2id="s2id-1", lat=HIGH_SEAS_LAT, lon=HIGH_SEAS_LON, top_destination=None):
    """A row shaped like the anchorage locations query's output."""
    return dict(
        s2id=s2id,
        lat=lat,
        lon=lon,
        total_visits=10,
        drift_radius=0.1,
        top_destination=top_destination,
        unique_stationary_ssvid=3,
        unique_stationary_fishing_ssvid=1,
        unique_active_ssvid=5,
        unique_total_ssvid=6,
        active_ssvid_days=2.0,
        stationary_ssvid_days=1.5,
        stationary_fishing_ssvid_days=0.5,
    )


def named_anchorage(**kwargs):
    """A NamedAnchoragePoint with no names yet, built from an anchorage_location(**kwargs) row."""
    return NamedAnchoragePoint.from_msg(anchorage_location(**kwargs))
