from .utils import SchemaBuilder

def build():

    builder = SchemaBuilder()

    builder.add("seg_id", "STRING")
    builder.add("timestamp", "TIMESTAMP")
    builder.add("lat", "FLOAT")
    builder.add("lon", "FLOAT")
    builder.add("vessel_lat", "FLOAT")
    builder.add("vessel_lon", "FLOAT")
    builder.add("anchorage_id", "STRING")
    builder.add("event_type", "STRING")
    builder.add("last_timestamp", "TIMESTAMP", mode="NULLABLE")

    return builder.schema



def build_event_state_schema():

    builder = SchemaBuilder()

    builder.add("seg_id", "STRING")
    builder.add("date", "TIMESTAMP")
    builder.add("state", "STRING", mode="NULLABLE")
    builder.add("active_port", "STRING", mode="NULLABLE")
    builder.add("last_timestamp", "TIMESTAMP", mode="NULLABLE")

    return builder.schema