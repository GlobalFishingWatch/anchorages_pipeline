from .utils import SchemaBuilder


def build():

    builder = SchemaBuilder()

    builder.add("s2id", "STRING", description="anchorage ID (id of the S2 grid cell)")
    builder.add("lat", "FLOAT", description="latitude of t'NULLABLE',he anchorage")
    builder.add("lon", "FLOAT", description="longitude of the anchorage")
    builder.add("label", "STRING", "NULLABLE", description="port name")
    builder.add("sublabel", "STRING", "NULLABLE", description="anchorage name")
    builder.add("iso3", "STRING", "NULLABLE", description="ISO3 of EEZ containing anchorage")
    builder.add(
        "label_source",
        "STRING",
        "NULLABLE",
        description="source of the label name (WPI, GeoNames, KKP, top destination)",
    )
    builder.add(
        "total_visits", "INTEGER", "NULLABLE", description="anchorage distance from shore (meters)"
    )
    builder.add(
        "drift_radius", "FLOAT", "NULLABLE", description="drift radius of vessels at the anchorage"
    )
    builder.add(
        "top_destination",
        "STRING",
        "NULLABLE",
        description="top destination reported in AIS messages from vessels arriving at the anchorage",
    )
    builder.add(
        "unique_stationary_ssvid",
        "INTEGER",
        "NULLABLE",
        description="number of unique vessels that have stopped at the anchorage",
    )
    builder.add(
        "unique_stationary_fishing_ssvid",
        "INTEGER",
        "NULLABLE",
        description="number of unique fishing vessels that have stopped at the anchorage",
    )
    builder.add(
        "unique_active_ssvid",
        "INTEGER",
        "NULLABLE",
        description="number of unique vessels that passed through the anchorage",
    )
    builder.add(
        "unique_total_ssvid",
        "INTEGER",
        "NULLABLE",
        description="number of unique vessels that have visited the anchorage",
    )
    builder.add(
        "active_ssvid_days",
        "FLOAT",
        "NULLABLE",
        description="number of days a vessel passed through the anchorage",
    )
    builder.add(
        "stationary_ssvid_days",
        "FLOAT",
        "NULLABLE",
        description="number of days a vessel was stopped at the anchorage",
    )
    builder.add(
        "stationary_fishing_ssvid_days",
        "FLOAT",
        "NULLABLE",
        description="number of days a fishing vessel was stopped at the anchorage",
    )

    return builder.schema
