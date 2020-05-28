from .utils import SchemaBuilder
from .port_event import build as build_port_event_schema

def build():

    builder = SchemaBuilder()

    builder.add("visit_id", "STRING")
    builder.add("track_id", "STRING")
    builder.add("start_timestamp", "TIMESTAMP")
    builder.add("start_lat", "FLOAT")
    builder.add("start_lon", "FLOAT")
    builder.add("start_anchorage_id", "STRING")
    builder.add("end_timestamp", "TIMESTAMP")
    builder.add("end_lat", "FLOAT")
    builder.add("end_lon", "FLOAT")
    builder.add("end_anchorage_id", "STRING")
    builder.add("events", mode="REPEATED", 
        schema_type=build_port_event_schema().fields
    )

    return builder.schema
