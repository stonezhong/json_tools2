import pytest

from json_tools2 import infer_schema, SchemaError
import json

def test_primitive_null():
    assert infer_schema(None) == {'type': 'null'}

def test_primitive_string():
    assert infer_schema("") == {'type': 'string'}

def test_primitive_number():
    assert infer_schema(1.0) == {'type': 'number'}

def test_primitive_integer():
    assert infer_schema(1) == {'type': 'integer'}

def test_primitive_boolean():
    assert infer_schema(True) == {'type': 'boolean'}
    assert infer_schema(False) == {'type': 'boolean'}

def test_object_empty():
    assert infer_schema({}) == {
        'type': 'object', 
        'properties': {}
    }

def test_object_null():
    assert infer_schema({"foo": None}) == {
        'type': 'object', 
        'properties': {
            'foo': {'type': 'null'}
        }
    }

def test_object_string():
    assert infer_schema({"foo": ""}) == {
        'type': 'object', 
        'properties': {
            'foo': {'type': 'string'}
        }
    }

def test_object_number():
    assert infer_schema({"foo": 1.0}) == {
        'type': 'object', 
        'properties': {
            'foo': {'type': 'number'}
        }
    }

def test_object_integer():
    assert infer_schema({"foo": 1}) == {
        'type': 'object', 
        'properties': {
            'foo': {'type': 'integer'}
        }
    }

def test_object_bool():
    assert infer_schema({"foo": True}) == {
        'type': 'object', 
        'properties': {
            'foo': {'type': 'boolean'}
        }
    }
    assert infer_schema({"foo": False}) == {
        'type': 'object', 
        'properties': {
            'foo': {'type': 'boolean'}
        }
    }

def test_object_object():
    assert infer_schema({"foo": {}}) == {
        'type': 'object', 
        'properties': {
            'foo': {
                'type': 'object', 
                'properties': {}
            }
        }
    }

def test_object_array():
    assert infer_schema({"foo": []}) == {
        'type': 'object', 
        'properties': {
            'foo': {
                'type': 'array', 
                'items': {"type": "null"}
            }
        }
    }

def test_array_empty():
    # empty array: we do not know item type, so we assume it is null
    assert infer_schema([]) == {
        'type': 'array', 
        'items': {'type': 'null'}
    }

def test_array_null():
    assert infer_schema([None]) == {
        'type': 'array', 
        'items': {'type': 'null'}
    }

def test_array_string():
    assert infer_schema([""]) == {
        'type': 'array', 
        'items': {'type': 'string'}
    }

def test_array_number():
    assert infer_schema([1.0]) == {
        'type': 'array', 
        'items': {'type': 'number'}
    }

def test_array_integer():
    assert infer_schema([1]) == {
        'type': 'array', 
        'items': {'type': 'integer'}
    }

def test_array_boolean():
    assert infer_schema([True]) == {'type': 'array', 'items': {'type': 'boolean'}}
    assert infer_schema([False]) == {
        'type': 'array', 
        'items': {'type': 'boolean'}
    }

def test_array_object():
    assert infer_schema([{}]) == {
        'type': 'array', 
        'items': {
            'type': 'object', 
            'properties': {}
        }
    }

def test_array_array():
    assert infer_schema([[]]) == {
        "type": "array",
        "items": {
            "type": "array",
            "items": {
                "type": "null"
            }
        }
    }

def test_invalid_json():
    o = object()
    with pytest.raises(SchemaError) as e_info:
        infer_schema({"foo": o})
    assert e_info.value.args[0] == f"{type(o)} is not a valid JSON value"

    o = (1,2)
    with pytest.raises(SchemaError) as e_info:
        infer_schema({"foo": o})
    assert e_info.value.args[0] == f"{type(o)} is not a valid JSON value"

def test_merge_null_null():
    assert infer_schema([None, None]) == {
        "type": "array",
        "items": {"type":"null"}
    }

def test_merge_null_string():
    assert infer_schema([None, ""]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'string'}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_null_number():
    assert infer_schema([None, 1.0]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'number'}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_null_integer():
    assert infer_schema([None, 1]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'integer'}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_null_boolean():
    assert infer_schema([None, True]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'boolean'}, 
                {'type': 'null'}
            ]
        }
    }
    assert infer_schema([None, False]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'boolean'}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_null_object():
    assert infer_schema([None, {}]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {
                    'type': 'object',
                    'properties': {}
                }, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_null_array():
    assert infer_schema([None, []]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {
                    'type': 'array',
                    'items': {'type': 'null'}
                }, 
                {'type': 'null'}
            ]
        }
    }
    assert infer_schema([None, [1]]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {
                    'type': 'array',
                    'items': {'type': 'integer'}
                }, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_string_null():
    assert infer_schema(["", None]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'string'}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_string_string():
    assert infer_schema(["", ""]) == {
        "type": "array",
        'items': {'type': 'string'}
    }

def test_merge_string_number():
    with pytest.raises(SchemaError) as e_info:
        infer_schema(["", 1.0])
    assert e_info.value.args[0] == "Cannot merge NUMBER to STRING"

def test_merge_string_integer():
    with pytest.raises(SchemaError) as e_info:
        infer_schema(["", 1])
    assert e_info.value.args[0] == "Cannot merge INTEGER to STRING"

def test_merge_string_boolean():
    with pytest.raises(SchemaError) as e_info:
        infer_schema(["", True])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to STRING"

    with pytest.raises(SchemaError) as e_info:
        infer_schema(["", False])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to STRING"

def test_merge_string_object():
    with pytest.raises(SchemaError) as e_info:
        infer_schema(["", {}])
    assert e_info.value.args[0] == "Cannot merge OBJECT to STRING"


def test_merge_string_array():
    with pytest.raises(SchemaError) as e_info:
        infer_schema(["", []])
    assert e_info.value.args[0] == "Cannot merge ARRAY to STRING"

def test_merge_number_null():
    assert infer_schema([1.0, None]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'number'}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_number_string():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([1.0, ""])
    assert e_info.value.args[0] == "Cannot merge STRING to NUMBER"

def test_merge_number_number():
    assert infer_schema([1.0, 1.0]) == {
        "type": "array",
        'items': {'type': 'number'}
    }

def test_merge_number_integer():
    assert infer_schema([1.0, 1]) == {
        "type": "array",
        'items': {'type': 'number'}
    }

def test_merge_number_boolean():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([1.0, True])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to NUMBER"

    with pytest.raises(SchemaError) as e_info:
        infer_schema([1.0, False])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to NUMBER"

def test_merge_number_object():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([1.0, {}])
    assert e_info.value.args[0] == "Cannot merge OBJECT to NUMBER"

def test_merge_number_array():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([1.0, []])
    assert e_info.value.args[0] == "Cannot merge ARRAY to NUMBER"

def test_merge_integer_null():
    assert infer_schema([1, None]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'integer'}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_integer_string():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([1, ""])
    assert e_info.value.args[0] == "Cannot merge STRING to INTEGER"

def test_merge_integer_number():
    assert infer_schema([1, 1.0]) == {
        "type": "array",
        'items': {'type': 'number'}
    }

def test_merge_integer_integer():
    assert infer_schema([1, 1]) == {
        "type": "array",
        'items': {'type': 'integer'}
    }

def test_merge_integer_boolean():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([1, True])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to INTEGER"

    with pytest.raises(SchemaError) as e_info:
        infer_schema([1, False])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to INTEGER"

def test_merge_integer_object():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([1, {}])
    assert e_info.value.args[0] == "Cannot merge OBJECT to INTEGER"

def test_merge_integer_object():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([1, []])
    assert e_info.value.args[0] == "Cannot merge ARRAY to INTEGER"

def test_merge_boolean_null():
    assert infer_schema([True, None]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'boolean'}, 
                {'type': 'null'}
            ]
        }
    }

    assert infer_schema([False, None]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'boolean'}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_boolean_string():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([True, ""])
    assert e_info.value.args[0] == "Cannot merge STRING to BOOLEAN"


    with pytest.raises(SchemaError) as e_info:
        infer_schema([False, ""])
    assert e_info.value.args[0] == "Cannot merge STRING to BOOLEAN"

def test_merge_boolean_number():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([True, 1.0])
    assert e_info.value.args[0] == "Cannot merge NUMBER to BOOLEAN"

    with pytest.raises(SchemaError) as e_info:
        infer_schema([False, 1.0])
    assert e_info.value.args[0] == "Cannot merge NUMBER to BOOLEAN"

def test_merge_boolean_integer():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([True, 1])
    assert e_info.value.args[0] == "Cannot merge INTEGER to BOOLEAN"

    with pytest.raises(SchemaError) as e_info:
        infer_schema([False, 1])
    assert e_info.value.args[0] == "Cannot merge INTEGER to BOOLEAN"

def test_merge_boolean_boolean():
    assert infer_schema([True, True]) == {
        "type": "array",
        'items': {'type': 'boolean'}
    }
    assert infer_schema([True, False]) == {
        "type": "array",
        'items': {'type': 'boolean'}
    }
    assert infer_schema([False, True]) == {
        "type": "array",
        'items': {'type': 'boolean'}
    }
    assert infer_schema([False, False]) == {
        "type": "array",
        'items': {'type': 'boolean'}
    }

def test_merge_boolean_object():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([True, {}])
    assert e_info.value.args[0] == "Cannot merge OBJECT to BOOLEAN"

    with pytest.raises(SchemaError) as e_info:
        infer_schema([False, {}])
    assert e_info.value.args[0] == "Cannot merge OBJECT to BOOLEAN"

def test_merge_boolean_array():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([True, []])
    assert e_info.value.args[0] == "Cannot merge ARRAY to BOOLEAN"

    with pytest.raises(SchemaError) as e_info:
        infer_schema([False, []])
    assert e_info.value.args[0] == "Cannot merge ARRAY to BOOLEAN"

def test_merge_object_null():
    assert infer_schema([{}, None]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'object', 'properties': {}}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_object_string():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([{}, ""])
    assert e_info.value.args[0] == "Cannot merge STRING to OBJECT"

def test_merge_object_number():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([{}, 1.0])
    assert e_info.value.args[0] == "Cannot merge NUMBER to OBJECT"

def test_merge_object_integer():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([{}, 1])
    assert e_info.value.args[0] == "Cannot merge INTEGER to OBJECT"

def test_merge_object_boolean():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([{}, True])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to OBJECT"

    with pytest.raises(SchemaError) as e_info:
        infer_schema([{}, False])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to OBJECT"

def test_merge_object_object_1():
    # discover new property
    assert infer_schema([{"foo": ""}, {"bar": 1}]) == {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "foo": {"type": "string"},
                "bar": {"type": "integer"}
            }
        }
    }

def test_merge_object_object_2():
    # merge existing property
    assert infer_schema([{"foo": ""}, {"foo": None}]) == {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "foo": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "null"},
                    ]
                }
            }
        }
    }

def test_merge_object_object_3():
    # merge existing property failed
    with pytest.raises(SchemaError) as e_info:
        infer_schema([{"foo": ""}, {"foo": 1}])
    assert e_info.value.args[0] == "Cannot merge INTEGER to STRING"

def test_merge_array_null():
    assert infer_schema([[], None]) == {
        "type": "array",
        'items': {
            'anyOf': [
                {'type': 'array', 'items': {'type':'null'}}, 
                {'type': 'null'}
            ]
        }
    }

def test_merge_array_string():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([[], ""])
    assert e_info.value.args[0] == "Cannot merge STRING to ARRAY"

def test_merge_array_number():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([[], 1.0])
    assert e_info.value.args[0] == "Cannot merge NUMBER to ARRAY"

def test_merge_array_integer():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([[], 1])
    assert e_info.value.args[0] == "Cannot merge INTEGER to ARRAY"

def test_merge_array_boolean():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([[], True])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to ARRAY"

    with pytest.raises(SchemaError) as e_info:
        infer_schema([[], False])
    assert e_info.value.args[0] == "Cannot merge BOOLEAN to ARRAY"

def test_merge_array_object():
    with pytest.raises(SchemaError) as e_info:
        infer_schema([[], {}])
    assert e_info.value.args[0] == "Cannot merge OBJECT to ARRAY"

def test_merge_array_array():
    assert infer_schema([[], []]) == {
        "type": "array",
        'items': {
            'type': 'array', 
            'items': {'type': 'null'}
        }
    }
