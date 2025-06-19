import datetime as dt
import logging
from datetime import timedelta

from apache_beam import Map, PTransform, io
from apache_beam.transforms.window import TimestampedValue
from google.cloud import bigquery
from pipe_anchorages.objects.namedtuples import epoch
from pipe_anchorages.schema.message_schema import message_schema
from pipe_anchorages.schema.named_anchorage import build as build_named_anchorage_schema
from pipe_anchorages.utils.ver import get_pipe_ver
from pipe_anchorages.schema.port_visit import port_visit_schema


def cloud_to_labels(ll): return {x.split("=")[0]: x.split("=")[1] for x in ll}


def get_table(bqclient, project: str, tablename: str):
    dataset_id, table_name = tablename.split(".")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_name)
    return bqclient.get_table(table_ref)  # API request


def load_labels(project: str, tablename: str, labels: dict):
    bqclient = bigquery.Client(project=project)
    table = get_table(bqclient, project, tablename)
    table.labels = labels
    bqclient.update_table(table, ["labels"])  # API request
    logging.info(f"Update labels to output table <{table}>")


class MessageSink(PTransform):
    def __init__(self, table, key="timestamp"):
        self.table = table
        self.key = key

    def compute_table_for_event(self, event):
        stamp = dt.date.fromtimestamp(event[self.key])
        return f"{self.table}{stamp:%Y%m%d}"

    def extract_latlon(self, x):
        x = x.copy()
        lonlat = x.pop("location")
        x["lon"] = lonlat.lon
        x["lat"] = lonlat.lat
        return x

    def as_dict(self, x):
        return x._asdict()

    def encode_datetimes_to_s(self, x):
        for field in [self.key]:
            if x[field] is not None:
                x[field] = (x[field] - epoch).total_seconds()
        return x

    def expand(self, xs):
        sink = io.WriteToBigQuery(
            self.compute_table_for_event,
            schema=message_schema,
            write_disposition=io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=io.BigQueryDisposition.CREATE_NEVER,
        )

        return (
            xs
            | Map(self.as_dict)
            | Map(self.encode_datetimes_to_s)
            | Map(self.extract_latlon)
            | Map(lambda x: TimestampedValue(x, x[self.key]))
            | sink
        )


class AnchorageSink(PTransform):
    def __init__(self, table, args, cloud_options):
        self.table = table
        self.args = args
        self.write_disposition = io.BigQueryDisposition.WRITE_TRUNCATE
        self.ver = get_pipe_ver()
        self.labels = cloud_to_labels(cloud_options.labels) if cloud_options else None

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
        "lat": ["float", "The mean latitude where the anchorage happened."],
        "lon": ["float", "The mean longitude where the anchorage happened."],
        "total_visits": ["integer", "The total visits to the anchorage."],
        "drift_radius": ["float", "The rms drift radius."],
        "top_destination": ["string", "The top destination."],
        "unique_stationary_ssvid": ["integer", "The unique stationary ssvid."],
        "unique_stationary_fishing_ssvid": ["integer", "The unique stationary fishing ssvid."],
        "unique_active_ssvid": ["integer", "The unique active ssvid."],
        "unique_total_ssvid": ["integer", "The unique total ssvid."],
        "active_ssvid_days": ["float", "The active ssvid days."],
        "stationary_ssvid_days": ["float", "The stationary ssvid days."],
        "stationary_fishing_ssvid_days": ["float", "The stationary fishing ssvid days"],
        "s2id": ["string", "The s2id."],
    }

    def get_description(self):
        return f"""
Created by the anchorages_pipeline: {self.ver}.
Creates the anchorage table.
* https://github.com/GlobalFishingWatch/anchorages_pipeline
* Sources: {self.args.input_table}
* Configuration: {self.args.config}
* Shapefile used: {self.args.shapefile}
        """

    def update_labels(self):
        load_labels(self.project, self.table, self.labels)

    @property
    def schema(self):
        def build_table_schema(spec):
            schema = io.gcp.internal.clients.bigquery.TableSchema()

            for name, fieldspec in spec.items():
                type, description = fieldspec
                field = io.gcp.internal.clients.bigquery.TableFieldSchema()
                field.name = name
                field.type = type
                field.mode = "nullable"
                field.description = description
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
                additional_bq_parameters={
                    "destinationTableProperties": {
                        "description": self.get_description(),
                    },
                },
            )
        )


class NamedAnchorageSink(PTransform):
    def __init__(self, table, args, cloud_options):
        self.table = table
        self.args = args
        self.write_disposition = io.BigQueryDisposition.WRITE_TRUNCATE
        self.ver = get_pipe_ver()
        self.labels = cloud_to_labels(cloud_options.labels) if cloud_options else None

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

    def get_description(self):
        return f"""
Created by the anchorages_pipeline: {self.ver}.
Creates the named anchorage table.
* https://github.com/GlobalFishingWatch/anchorages_pipeline
* Sources: {self.args.input_table}
* Configuration: {self.args.config}
* Shapefile used: {self.args.shapefile}
        """

    def update_labels(self):
        load_labels(self.project, self.table, self.labels)

    def expand(self, xs):
        return (
            xs
            | Map(self.encode)
            | io.WriteToBigQuery(
                table=self.table,
                write_disposition=self.write_disposition,
                schema=self.schema,
                additional_bq_parameters={
                    "destinationTableProperties": {
                        "description": self.get_description(),
                    },
                },
            )
        )


class VisitsSink(PTransform):
    def __init__(self, table, key="end_timestamp"):
        self.table = table
        self.key = key

    def expand(self, xs):
        return xs | io.WriteToBigQuery(
            table=self.table,
            schema=port_visit_schema,
            write_disposition= io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=io.BigQueryDisposition.CREATE_NEVER,
        )
