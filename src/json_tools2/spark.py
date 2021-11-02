from pyspark.sql.types import StructType, StructField, ArrayType, \
    StringType, BooleanType, TimestampType, DoubleType, LongType

# convert a json schema to spark type
def get_py_schema_from_json_schema(json_schema):
    type = json_schema['type']
    if type == 'string':
        if "is_datetime_string" in json_schema:
            return TimestampType()
        else:
            return StringType()
    if type == 'integer':
        return LongType()
    if type == 'number':
        return DoubleType()
    if type == 'boolean':
        return BooleanType()
    if type == "object":
        fields = []
        for name, sub_schema in json_schema['properties'].items():
            sub_type = get_py_schema_from_json_schema(sub_schema)
            fields.append(StructField(name, sub_type, nullable=True))
        return StructType(fields)
    if type == "array":
        element_type = get_py_schema_from_json_schema(json_schema['items'])
        return ArrayType(element_type, True)
    raise Exception(f"Unsupported schema: {type}")

