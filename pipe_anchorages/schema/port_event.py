from .utils import SchemaBuilder

def build():

    builder = SchemaBuilder()

    builder.add("vessel_id", "STRING")
    builder.add("timestamp", "TIMESTAMP")
    builder.add("lat", "FLOAT")
    builder.add("lon", "FLOAT")
    builder.add("vessel_lat", "FLOAT")
    builder.add("vessel_lon", "FLOAT")
    builder.add("anchorage_id", "STRING")
    builder.add("port_label", "STRING")
    builder.add("event_type", "STRING")

    return builder.schema