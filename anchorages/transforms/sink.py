from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io


class EventSink(PTransform):
    def __init__(self, table, write_disposition):
        self.table = table
        self.write_disposition = write_disposition

    def expand(self, xs):
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

        return xs | Map(encode_datetimes_to_iso) | io.Write(io.gcp.bigquery.BigQuerySink(
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


# def anchorage_point_to_json(a_pt):
#     return json.dumps({
#         'lat' : a_pt.mean_location.lat, 
#         'lon': a_pt.mean_location.lon,
#         'total_visits' : a_pt.total_visits,
#         'drift_radius' : a_pt.rms_drift_radius,
#         'destinations': a_pt.top_destinations,
#         'unique_stationary_mmsi' : len(a_pt.vessels),
#         'unique_stationary_fishing_mmsi' : len(a_pt.fishing_vessels),
#         'unique_active_mmsi' : a_pt.active_mmsi,
#         'unique_total_mmsi' : a_pt.total_mmsi,
#         'active_mmsi_days': a_pt.active_mmsi_days,
#         'stationary_mmsi_days': a_pt.stationary_mmsi_days,
#         'stationary_fishing_mmsi_days': a_pt.stationary_fishing_mmsi_days,
#         'port_name': a_pt.port_name,
#         'port_distance': a_pt.port_distance,
#         's2id' : a_pt.s2id
#         })


class AnchorageSink(PTransform):
    def __init__(self, table, write_disposition):
        self.table = table
        self.write_disposition = write_disposition

    def expand(self, xs):

        def build_table_schema(spec):
            schema = io.gcp.internal.clients.bigquery.TableSchema()

            for name, type in spec.iteritems():
                field = io.gcp.internal.clients.bigquery.TableFieldSchema()
                field.name = name
                field.type = type
                field.mode = 'nullable'
                schema.fields.append(field)

            return schema

        return xs | Map(encode_datetimes_to_iso) | io.Write(io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=build_table_schema({
                "lat": "float",
                "lon": "float",
                "anchorage_id": "string",
                "port_label": "string",
                "event_type": "string"
            })
        ))