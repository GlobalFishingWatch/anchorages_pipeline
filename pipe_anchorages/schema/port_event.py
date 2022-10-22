from .utils import SchemaBuilder


def build():

    builder = SchemaBuilder()

    builder.add("ssvid", "STRING")
    builder.add("seg_id", "STRING")
    builder.add("timestamp", "TIMESTAMP")
    builder.add("lat", "FLOAT")
    builder.add("lon", "FLOAT")
    builder.add("vessel_lat", "FLOAT", mode="NULLABLE")
    builder.add("vessel_lon", "FLOAT", mode="NULLABLE")
    builder.add("anchorage_id", "STRING")
    builder.add("event_type", "STRING")
    builder.add("last_timestamp", "TIMESTAMP", mode="NULLABLE")

    return builder.schema
