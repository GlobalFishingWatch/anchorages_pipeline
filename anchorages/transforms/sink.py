from apache_beam import PTransform
from apache_beam import Map
from apache_beam import io


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

        def encode(anchorage):
            x = event._asdict()
            x['unique_stationary_mmsi'] = len(x.pop('vessels'))
            x['unique_stationary_fishing_mmsi'] = len(x.pop('fishing_vessels'))


        spec = {
                "lat": "float",
                "lon": "float",
                "total_visits": "integer",
                "drift_radius": "float",
                "unique_stationary_mmsi": "integer",
                "unique_active_mmsi": "integer",
                "stationary_mmsi_days": "integer",
                "stationary_fishing_mmsi_days": "integer",
                "s2id": "string",
                "top_destination": "string",
                "wpi_name": "string",
                "wpi_distance": "float",
                "geonames_name": "string",
                "geonames_distance": "float",
            }
                    
        return xs | Map(encode) | io.Write(io.gcp.bigquery.BigQuerySink(
            table=self.table,
            write_disposition=self.write_disposition,
            schema=build_table_schema(spec)
            ))