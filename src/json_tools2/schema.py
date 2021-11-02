#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from copy import deepcopy

#############################################################
# Infer schema from bunch of json objects
# objs: iterable of json
#############################################################
def infer_schema(objs, schema=None):
    if schema is None:
        ret_schema = {
            "type": "object",
            "properties": {}
        }
    else:
        ret_schema = deepcopy(schema)
    for obj in objs:
        _update_schema(ret_schema, obj)
    return ret_schema

def merge_schema(schema, new_schema):
    merged_schema = deepcopy(schema)
    _update_schema(merged_schema, _create_example_from_schema(new_schema))
    return merged_schema

class SchemaError(Exception):
    pass


def _update_schema_for_primitive_value(schema, value):
    if type(value) == str:
        if schema["type"] == "string":
            # good
            pass
        elif schema["type"] == "null":
            # upgrade
            schema["type"] = "string"
        else:
            raise SchemaError()
    elif type(value) == int:
        if schema["type"] == "integer":
            # good
            pass
        elif schema["type"] == "null":
            # upgrade
            schema["type"] = "integer"
        else:
            raise SchemaError()
    elif type(value) == float:
        if schema["type"] == "number":
            # good
            pass
        elif schema["type"] in ["null", "integer"]:
            # upgrade
            schema["type"] = "number"
        else:
            raise SchemaError()
    elif type(value) == bool:
        if schema["type"] == "boolean":
            # good
            pass
        elif schema["type"] == "null":
            schema["type"] = "boolean"
        else:
            raise SchemaError()


# update schema with object
def _update_schema(schema, obj):
    if type(obj) == dict:
        if schema["type"] == "object":
            # good
            pass
        elif schema["type"] == "null":
            # upgrade
            schema.update({
                "type": "object",
                "properties": {}
            })
        else:
            raise SchemaError()
        properties = schema["properties"]
        for key, value in obj.items():
            if key not in properties:
                # set type to null first, will upgrade with value latter
                properties[key] = {"type": "null"}
            field_type = properties[key]

            if value is None:
                # null is compatible with any type
                pass
            elif type(value) in [str, int, float, bool]:
                _update_schema_for_primitive_value(field_type, value)
            elif type(value) == dict:
                _update_schema(field_type, value)
            elif type(value) == list:
                _update_schema(field_type, value)
            else:
                raise Exception("Unrecognized type")
    elif type(obj) == list:
        if schema["type"] == "array":
            # good
            pass
        elif schema["type"] == "null":
            # upgrade
            schema.update({
                "type": "array",
                "items": {
                    "type": "null"
                }
            })
        else:
            raise SchemaError()
        items = schema["items"]
        for v in obj:
            if v is None:
                # null is compatible with any type
                pass
            elif type(v) in [str, int, float, bool]:
                _update_schema_for_primitive_value(items, v)
            elif type(v) == dict:
                _update_schema(items, v)
            elif type(v) == list:
                _update_schema(items, v)
            else:
                raise Exception("Unrecognized type")
    else:
        raise SchemaError()


def _create_example_from_schema(schema):
    if schema['type'] == 'string':
        return ""
    if schema['type'] == 'null':
        return None
    if schema['type'] == 'integer':
        return 1
    if schema['type'] == 'number':
        return 1.0
    if schema['type'] == 'boolean':
        return True
    if schema['type'] == 'object':
        ret = {}
        for property_name, property_schema in schema['properties'].items():
            ret[property_name] = _create_example_from_schema(property_schema)
        return ret
    if schema['type'] == 'array':
        ret = [_create_example_from_schema(schema['items'])]
        return ret
    raise SchemaError()
    

