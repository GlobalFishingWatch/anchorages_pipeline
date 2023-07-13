from .utils import SchemaBuilder


def build():

    builder = SchemaBuilder()

    builder.add("ssvid", "STRING", description="The ssvid of the vessel involved in the port.")
    builder.add("seg_id", "STRING", description="The segment id belonging to the vessel.")
    builder.add("timestamp", "TIMESTAMP", description="The timestamp when the message was received.")
    builder.add("lat", "FLOAT", description="The latitude included in the message.")
    builder.add("lon", "FLOAT", description="The longitude included in the message.")
    builder.add("vessel_lat", "FLOAT", mode="NULLABLE", description="The latitude of the vessel.")
    builder.add("vessel_lon", "FLOAT", mode="NULLABLE", description="The longitude of the vessel.")
    builder.add("anchorage_id", "STRING", description="The id of the anchorage.")
    builder.add("event_type", "STRING", description="The event type.")
    builder.add("last_timestamp", "TIMESTAMP", mode="NULLABLE", description="The last timestamp")

    return builder.schema
