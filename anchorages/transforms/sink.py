from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io


#TODO: refactor

class EventSink(PTransform):
    def __init__(self, table, write_disposition):
        self.table = table
        self.write_disposition = write_disposition

    def expand(self, xs):

        def as_dict(x):
            return x._asdict()

        def encode_datetimes_to_iso(x):
            def encode_datetime_field(value):
                return value.strftime('%Y-%m-%d %H:%M:%S.%f UTC')

            for field in ['timestamp']:
                x[field] = encode_datetime_field(x[field])

            return x

        def build_table_schema(spec):
            schema = io.gcp.internal.clients.bigquery.TableSchema()

            for name, type in spec.iteritems():
                field = io.gcp.internal.clients.bigquery.TableFieldSchema()
                field.name = name
                field.type = type
                field.mode = 'nullable'
                schema.fields.append(field)

            return schema

        return xs | Map(as_dict)| Map(encode_datetimes_to_iso) | io.Write(io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=build_table_schema({
                "mmsi": "integer",
                "timestamp": "timestamp",
                "lat": "float",
                "lon": "float",
                "vessel_lat": "float",
                "vessel_lon": "float",
                "anchorage_id": "string",
                "port_label": "string",
                "event_type": "string"
            })
        ))



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
            's2id' : anchorage.s2id,        
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