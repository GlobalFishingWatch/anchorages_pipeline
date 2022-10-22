import datetime
import logging

from apache_beam import Map, PTransform, io
from apache_beam.transforms.window import TimestampedValue

from ..objects.namedtuples import epoch
from ..schema.message_schema import message_schema
from ..schema.named_anchorage import build as build_named_anchorage_schema


class MessageSink(PTransform):
    def __init__(self, table, temp_location, project):
        self.table = table
        self.temp_location = temp_location
        self.project = project

    def compute_table_for_event(self, event):
        stamp = datetime.date.fromtimestamp(event["timestamp"])
        return f"{self.project}:{self.table}{stamp:%Y%m%d}"

    def extract_latlon(self, x):
        x = x.copy()
        lonlat = x.pop("location")
        x["lon"] = lonlat.lon
        x["lat"] = lonlat.lat
        return x

    def expand(self, xs):
        def as_dict(x):
            d = x._asdict()
            return d

        def encode_datetimes_to_s(x):

            for field in ["timestamp"]:
                if x[field] is not None:
                    x[field] = (x[field] - epoch).total_seconds()

            return x

        sink = io.WriteToBigQuery(
            self.compute_table_for_event,
            schema=message_schema,
            write_disposition="WRITE_TRUNCATE",
        )

        logging.info(
            "sink params: \n\t%s\n\t%s\n\t%s",
            self.temp_location,
            self.table,
            self.project,
        )

        return (
            xs
            | Map(as_dict)
            | Map(encode_datetimes_to_s)
            | Map(self.extract_latlon)
            | Map(lambda x: TimestampedValue(x, x["timestamp"]))
            | sink
        )


class AnchorageSink(PTransform):
    def __init__(self, table, write_disposition):
        self.table = table
        self.write_disposition = write_disposition

    def encode(self, anchorage):
        return {
            "lat": anchorage.mean_location.lat,
            "lon": anchorage.mean_location.lon,
            "total_visits": anchorage.total_visits,
            "drift_radius": anchorage.rms_drift_radius,
            "top_destination": anchorage.top_destination,
            "unique_stationary_ssvid": len(anchorage.vessels),
            "unique_stationary_fishing_ssvid": len(anchorage.fishing_vessels),
            "unique_active_ssvid": anchorage.active_ssvids,
            "unique_total_ssvid": anchorage.total_ssvids,
            "active_ssvid_days": anchorage.active_ssvid_days,
            "stationary_ssvid_days": anchorage.stationary_ssvid_days,
            "stationary_fishing_ssvid_days": anchorage.stationary_fishing_ssvid_days,
            "s2id": anchorage.s2id,
        }

    spec = {
        "lat": "float",
        "lon": "float",
        "total_visits": "integer",
        "drift_radius": "float",
        "top_destination": "string",
        "unique_stationary_ssvid": "integer",
        "unique_stationary_fishing_ssvid": "integer",
        "unique_active_ssvid": "integer",
        "unique_total_ssvid": "integer",
        "active_ssvid_days": "float",
        "stationary_ssvid_days": "float",
        "stationary_fishing_ssvid_days": "float",
        "s2id": "string",
    }

    @property
    def schema(self):
        def build_table_schema(spec):
            schema = io.gcp.internal.clients.bigquery.TableSchema()

            for name, type in spec.items():
                field = io.gcp.internal.clients.bigquery.TableFieldSchema()
                field.name = name
                field.type = type
                field.mode = "nullable"
                schema.fields.append(field)

            return schema

        return build_table_schema(self.spec)

    def expand(self, xs):
        return (
            xs
            | Map(self.encode)
            | io.WriteToBigQuery(
                table=self.table,
                write_disposition=self.write_disposition,
                schema=self.schema,
            )
        )


class NamedAnchorageSink(PTransform):
    def __init__(self, table, write_disposition):
        self.table = table
        self.write_disposition = write_disposition

    def encode(self, anchorage):
        return {
            "lat": anchorage.mean_location.lat,
            "lon": anchorage.mean_location.lon,
            "total_visits": anchorage.total_visits,
            "drift_radius": anchorage.rms_drift_radius,
            "top_destination": anchorage.top_destination,
            "unique_stationary_ssvid": len(anchorage.vessels),
            "unique_stationary_fishing_ssvid": len(anchorage.fishing_vessels),
            "unique_active_ssvid": anchorage.active_ssvids,
            "unique_total_ssvid": anchorage.total_ssvids,
            "active_ssvid_days": anchorage.active_ssvid_days,
            "stationary_ssvid_days": anchorage.stationary_ssvid_days,
            "stationary_fishing_ssvid_days": anchorage.stationary_fishing_ssvid_days,
            "s2id": anchorage.s2id,
            "label": anchorage.label,
            "sublabel": anchorage.sublabel,
            "label_source": anchorage.label_source,
            "iso3": anchorage.iso3,
        }

    @property
    def schema(self):
        return build_named_anchorage_schema()

    def expand(self, xs):
        return (
            xs
            | Map(self.encode)
            | io.WriteToBigQuery(
                table=self.table,
                write_disposition=self.write_disposition,
                schema=self.schema,
            )
        )
