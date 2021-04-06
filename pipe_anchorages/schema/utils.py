from apache_beam.io.gcp.internal.clients import bigquery

class SchemaBuilder(object):

    allowed_types = {
        "INTEGER",
        "FLOAT",
        "TIMESTAMP",
        "STRING",
        "RECORD",
        "DATE"
    }

    def __init__(self):
        self.schema = bigquery.TableSchema()

    def build(self, name, schema_type, mode='REQUIRED', description=None):
        is_record = isinstance(schema_type, (list, tuple))
        type_name = 'RECORD' if is_record else schema_type
        if type_name not in self.allowed_types:
            raise ValueError('"{}" not in allowed_types'.format(type_name))
        if description is None:
            field = bigquery.TableFieldSchema()
        else:
            field = bigquery.TableFieldSchema(description=description)
        field.name = name
        field.type = type_name
        field.mode = mode
        if is_record:
            for subfield in schema_type:
                field.fields.append(subfield)
        return field   

    def add(self, name, schema_type, mode="REQUIRED", description=None):
        field = self.build(name, schema_type, mode, description)
        self.schema.fields.append(field)
        return field