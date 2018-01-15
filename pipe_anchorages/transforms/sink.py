import logging
import pytz
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io
from apache_beam.transforms.window import TimestampedValue
from pipe_tools.coders.jsoncoder import JSONDict
from pipe_tools.io import WriteToBigQueryDatePartitioned
from ..objects.namedtuples import epoch
from ..schema.port_event import build as build_event_schema



class EventSink(PTransform):
    def __init__(self, table, temp_location, project):
        self.table = table
        self.temp_location = temp_location
        self.project = project

    def expand(self, xs):

        def as_dict(x):
            return JSONDict(**x._asdict())

        def encode_datetimes_to_s(x):

            for field in ['timestamp']:
                x[field] = (x[field].replace(tzinfo=pytz.utc) - epoch).total_seconds()

            # logging.info("Encoded: %s", str(x))

            return x


        dataset, table = self.table.split('.')


        sink = WriteToBigQueryDatePartitioned(
            temp_gcs_location=self.temp_location,
            dataset=dataset,
            table=table,
            project=self.project,
            write_disposition="WRITE_TRUNCATE",
            schema=build_event_schema()
            )


        logging.info('sink params: \n\t%s\n\t%s\n\t%s\n\t%s', self.temp_location, dataset, table, self.project)

        return (xs 
            | Map(as_dict)
            | Map(encode_datetimes_to_s)
            | Map(lambda x: TimestampedValue(x, x['timestamp'])) 
            | sink
            )



class AnchorageSink(PTransform):
    def __init__(self, table, write_disposition):
        self.table = table
        self.write_disposition = write_disposition

    def encode(self, anchorage):
        return {
            'lat' : anchorage.mean_location.lat, 
            'lon': anchorage.mean_location.lon,
            'total_visits' : anchorage.total_visits,
            'drift_radius' : anchorage.rms_drift_radius,
            'top_destination': anchorage.top_destination,
            'unique_stationary_mmsi' : len(anchorage.vessels),
            'unique_stationary_fishing_mmsi' : len(anchorage.fishing_vessels),
            'unique_active_mmsi' : anchorage.active_mmsi,
            'unique_total_mmsi' : anchorage.total_mmsi,
            'active_mmsi_days': anchorage.active_mmsi_days,
            'stationary_mmsi_days': anchorage.stationary_mmsi_days,
            'stationary_fishing_mmsi_days': anchorage.stationary_fishing_mmsi_days,
            's2id': anchorage.s2id,   
            }


    spec = {
            "lat": "float",
            "lon": "float",
            "total_visits": "integer",
            "drift_radius": "float",
            "top_destination" : "string",
            "unique_stationary_mmsi": "integer",
            "unique_stationary_fishing_mmsi": "integer",
            "unique_active_mmsi": "integer",
            "unique_total_mmsi": "integer",
            'active_mmsi_days': "float",
            "stationary_mmsi_days": "float",
            "stationary_fishing_mmsi_days": "float",
            "s2id": "string",
        }


    @property
    def schema(self):

        def build_table_schema(spec):
            schema = io.gcp.internal.clients.bigquery.TableSchema()

            for name, type in spec.iteritems():
                field = io.gcp.internal.clients.bigquery.TableFieldSchema()
                field.name = name
                field.type = type
                field.mode = 'nullable'
                schema.fields.append(field)

            return schema   

        return build_table_schema(self.spec)

    def expand(self, xs):        
        return xs | Map(self.encode) | io.Write(io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=self.schema
            ))



class NamedAnchorageSink(PTransform):
    def __init__(self, table, write_disposition):
        self.table = table
        self.write_disposition = write_disposition

    def encode(self, anchorage):
        return {
            'lat' : anchorage.mean_location.lat, 
            'lon': anchorage.mean_location.lon,
            'total_visits' : anchorage.total_visits,
            'drift_radius' : anchorage.rms_drift_radius,
            'top_destination': anchorage.top_destination,
            'unique_stationary_mmsi' : len(anchorage.vessels),
            'unique_stationary_fishing_mmsi' : len(anchorage.fishing_vessels),
            'unique_active_mmsi' : anchorage.active_mmsi,
            'unique_total_mmsi' : anchorage.total_mmsi,
            'active_mmsi_days': anchorage.active_mmsi_days,
            'stationary_mmsi_days': anchorage.stationary_mmsi_days,
            'stationary_fishing_mmsi_days': anchorage.stationary_fishing_mmsi_days,
            's2id' : anchorage.s2id,  
            'label': anchorage.label,
            'sublabel': anchorage.sublabel, 
            'label_source': anchorage.label_source,
            'iso3': anchorage.iso3,  
            }


    spec = {
            "lat": "float",
            "lon": "float",
            "total_visits": "integer",
            "drift_radius": "float",
            "top_destination" : "string",
            "unique_stationary_mmsi": "integer",
            "unique_stationary_fishing_mmsi": "integer",
            "unique_active_mmsi": "integer",
            "unique_total_mmsi": "integer",
            'active_mmsi_days': "float",
            "stationary_mmsi_days": "float",
            "stationary_fishing_mmsi_days": "float",
            "s2id": "string",
            'label': 'string',
            'sublabel': 'string',
            'label_source': 'string',
            "iso3": "string",
        }


    @property
    def schema(self):

        def build_table_schema(spec):
            schema = io.gcp.internal.clients.bigquery.TableSchema()

            for name, type in spec.iteritems():
                field = io.gcp.internal.clients.bigquery.TableFieldSchema()
                field.name = name
                field.type = type
                field.mode = 'nullable'
                schema.fields.append(field)

            return schema   

        return build_table_schema(self.spec)

    def expand(self, xs):        
        return xs | Map(self.encode) | io.Write(io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=self.schema
            ))