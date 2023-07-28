#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from typing import Any, Optional, Dict
from enum import Enum

class _ValueType(Enum):
    NULL    = 0
    STRING  = 1
    NUMBER  = 2
    INTEGER = 3
    BOOLEAN = 4
    OBJECT  = 5
    ARRAY   = 6

    @classmethod
    def from_value(cls, value:Any):
        if value is None:
            return cls.NULL
        if type(value) == str:
            return cls.STRING
        if type(value) == float:
            return cls.NUMBER
        if type(value) == int:
            return cls.INTEGER
        if type(value) == bool:
            return cls.BOOLEAN
        if type(value) == list:
            return cls.ARRAY
        if type(value) == dict:
            return cls.OBJECT
        
        raise SchemaError(f"{type(value)} is not a valid JSON value")
    
class ValueSchema:
    value_type      : _ValueType
    item_type       :  Optional["ValueSchema"]
    property_types  : Optional[Dict[str, "ValueSchema"]]
    null_merged     : bool

    def __init__(
        self, 
        value_type:_ValueType, 
        item_type:Optional["ValueSchema"]=None,
        property_types:Optional[Dict[str, "ValueSchema"]]=None
    ):
        self.value_type     = value_type
        self.item_type      = item_type
        self.property_types = property_types

        # have we merged null?
        self.null_merged    = self.value_type == _ValueType.NULL
    
    def __str__(self) -> str:
        return f"value_type={self.value_type}, null_merged={self.null_merged}"

    def to_json(self) -> Any:
        if not self.null_merged or self.value_type == _ValueType.NULL:
            return self.to_json_raw()
        return {
            "anyOf": [
                self.to_json_raw(),
                {"type": "null"}
            ]
        }

    def clone(self) -> "ValueSchema":
        new_item_type = None if self.item_type is None else self.item_type.clone()
        if self.property_types is None:
            new_property_types = None
        else:
            new_property_types = {
                property_name:property_type.clone() for property_name, property_type \
                    in self.property_types.items()
            }
        return ValueSchema(self.value_type, item_type=new_item_type, property_types=new_property_types)

    def dump_json(self):
        return {
            "value_type": self.value_type.value,
            "item_type": None if self.item_type is None else self.item_type.dumps(),
            "property_types": None if self.property_types is None else \
                {
                    property_name: property_type.dumps() for property_name, property_type in self.property_types
                },
            "null_merged": self.null_merged
        }

    @classmethod
    def load_json(cls, payload):
        value_type = _ValueType(payload["value_type"])
        item_type = None if payload["item_type"] is None else cls.load_json(payload["item_type"])
        if payload["property_types"] is None:
            property_types = None
        else:
            property_types = {
                property_name: cls.load_json(property_type) \
                    for property_name, property_type in payload["property_types"].items()
            }
        ret = cls(value_type, item_type=item_type, property_types=property_types)
        ret.null_merged = payload["null_merged"]
        return ret
        

    def to_json_raw(self) -> Any:
        if self.value_type == _ValueType.NULL:
            return {"type":"null"}
        if self.value_type == _ValueType.STRING:
            return {"type":"string"}
        if self.value_type == _ValueType.NUMBER:
            return {"type":"number"}
        if self.value_type == _ValueType.INTEGER:
            return {"type":"integer"}
        if self.value_type == _ValueType.BOOLEAN:
            return {"type":"boolean"}
        if self.value_type == _ValueType.OBJECT:
            return {
                "type": "object", 
                "properties": {
                    property_name: property_type.to_json() for property_name, property_type \
                        in self.property_types.items()
                }
            }

        if self.value_type == _ValueType.ARRAY:
            return {
                "type": "array", 
                "items": {"type": "null" } if self.item_type is None \
                    else self.item_type.to_json()
            }

        assert False  # impossible
    
    def merge_from(self, schema:"ValueSchema") -> None:
        if self.value_type == _ValueType.NULL:
            # ok, switch to the new schema, null_merged should be set already            
            self.value_type = schema.value_type
            if schema.value_type == _ValueType.OBJECT:
                self.property_types = {
                    property_name: property_type.clone() for property_name, property_type \
                        in schema.property_types.items()
                }
            elif schema.value_type == _ValueType.ARRAY:
                if schema.item_type is not None:
                    self.item_type = schema.item_type.clone()
            return

        if self.value_type == _ValueType.STRING:
            # for string, we only expect null or string
            if schema.value_type == _ValueType.NULL:
                self.null_merged = True
                return
            if schema.value_type == _ValueType.STRING:
                return
            raise SchemaError(f"Cannot merge {schema.value_type.name} to STRING")
            
        if self.value_type == _ValueType.NUMBER:
            # for NUMBER, we only expect null or NUMBER or INTEGER
            if schema.value_type == _ValueType.NULL:
                self.null_merged = True
                return
            if schema.value_type in (_ValueType.NUMBER, _ValueType.INTEGER):
                return
            raise SchemaError(f"Cannot merge {schema.value_type.name} to NUMBER")

        if self.value_type == _ValueType.INTEGER:
            # for INTEGER, we only expect null or NUMBER or INTEGER
            if schema.value_type == _ValueType.NULL:
                self.null_merged = True
                return
            if schema.value_type == _ValueType.NUMBER:
                self.value_type = _ValueType.NUMBER
                return
            if schema.value_type == _ValueType.INTEGER:
                return
            raise SchemaError(f"Cannot merge {schema.value_type.name} to INTEGER")
    
        if self.value_type == _ValueType.BOOLEAN:
            # for BOOLEAN, we only expect null or BOOLEAN
            if schema.value_type == _ValueType.NULL:
                self.null_merged = True
                return
            if schema.value_type == _ValueType.BOOLEAN:
                return
            raise SchemaError(f"Cannot merge {schema.value_type.name} to BOOLEAN")

        if self.value_type == _ValueType.OBJECT:
            # for OBJECT, we only expect null or OBJECT
            if schema.value_type == _ValueType.NULL:
                self.null_merged = True
                return
            if schema.value_type == _ValueType.OBJECT:
                for property_name, property_type in schema.property_types.items():
                    if property_name in self.property_types:
                        self.property_types[property_name].merge_from(property_type)
                    else:
                        self.property_types[property_name] = property_type
                return
            raise SchemaError(f"Cannot merge {schema.value_type.name} to OBJECT")

        if self.value_type == _ValueType.ARRAY:
            # for ARRAY, we only expect null or ARRAY
            if schema.value_type == _ValueType.NULL:
                self.null_merged = True
                return
            if schema.value_type == _ValueType.ARRAY:
                if self.item_type is None:
                    self.item_type = schema.item_type
                else:
                    self.item_type.merge_from(schema.item_type)
                return
            raise SchemaError(f"Cannot merge {schema.value_type.name} to ARRAY")
        
        assert False # impossible


def infer_schema(value:Any) -> Any:
    return get_schema(value).to_json()


def get_schema(value:Any) -> ValueSchema:
    value_type = _ValueType.from_value(value)

    if value_type == _ValueType.OBJECT:
        obj_schema = ValueSchema(value_type=value_type, property_types={})
        for property_name, property_value in value.items():
            property_schema = get_schema(property_value)
            if property_name in obj_schema.property_types:
                obj_schema.property_types[property_name].merge_from(property_schema)
            else:
                obj_schema.property_types[property_name] = property_schema
        return obj_schema

    if value_type == _ValueType.ARRAY:
        array_schema = ValueSchema(value_type=value_type, item_type=None)
        for item in value:
            item_schema = get_schema(item)
            if array_schema.item_type is None:
                array_schema.item_type = item_schema
            else:
                array_schema.item_type.merge_from(item_schema)
        return array_schema

    # it must be primitive type
    return ValueSchema(value_type=value_type)  


class SchemaError(Exception):
    pass
