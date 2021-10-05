#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from typing import Dict
import json
import uuid
from copy import deepcopy, copy
import numbers

class Schema:
    def __init__(self, json_schema):
        # we will make a copy of json_schema so future update on json_schema won't impact us
        self.__json_schema  = deepcopy(json_schema)

        self.is_array       = isinstance(self.__json_schema, dict) and self.__json_schema['type'] == 'array'
        self.is_struct      = isinstance(self.__json_schema, dict) and self.__json_schema['type'] == 'struct'
        self.is_primitive   = isinstance(self.__json_schema, str)

        if isinstance(self.__json_schema, dict):
            self.type = self.__json_schema["type"]

            et = self.__json_schema.get("elementType", None)
            self.elementType = None if et is None else Schema(et)
            
            self.containsNull = self.__json_schema.get("containsNull")

            json_fields = self.__json_schema.get("fields")
            if json_fields is None:
                self.fields = None
            else:
                self.fields = []
                for json_field in json_fields:
                    self.fields.append(Field(json_field))
        elif isinstance(self.__json_schema, str):
            self.type = None
            self.elementType = None
            self.containsNull = None
            self.fields = None
        else:
            raise Exception(f"bad schema: {self.__json_schema}")
    
    @property
    def json_schema(self):
        return deepcopy(self.__json_schema)


    def add_field(self, json_field):
        field_name = json_field["name"]
        if field_name in [field.name for field in self.fields]:
            raise Exception(f"field {field_name} already exists")

        new_schema = self.json_schema
        new_schema["fields"].append(json_field)
        return Schema(new_schema)


    def __str__(self):
        return json.dumps(self.__json_schema, indent=2)


    def strfmt(self, indent="", prefix=""):
        if self.is_primitive:
            return f"{indent}{prefix}{self.__json_schema}\n"
        
        if self.is_array:
            if self.elementType.is_primitive:
                return self.elementType.strfmt(indent=indent, prefix=f"{prefix} []")
            if self.elementType.is_struct:
                s = f"{indent}{prefix}[] structure\n"
                for field in self.elementType.fields:
                    s += field.type.strfmt(indent=indent+"    ", prefix=f"{field.name}: ")
                return s
            if self.elementType.is_array:
                s = self.elementType.strfmt(indent="", prefix="")
                s = f"{indent}{prefix}[]{s}"
                return s
        
        if self.is_struct:
            s = f"{indent}{prefix}structure\n"
            for field in self.fields:
                s += field.type.strfmt(indent=indent+"    ", prefix=f"{field.name}: ")
            return s
        
        raise Exception("bad schema")

class Field:
    def __init__(self, json_field):
        # we will make a copy of json_field so future update on json_field won't impact us
        self.__json_field = deepcopy(json_field)

        self.metadata   = self.__json_field["metadata"]
        self.name       = self.__json_field["name"]
        self.nullable   = self.__json_field["nullable"]
        self.type       = Schema(self.__json_field['type'])
    
    @property
    def json_field(self):
        return deepcopy(self.__json_field)

    def __str__(self):
        return json.dumps(self.json_field, indent=2)

class FlatJson:
    def __init__(self, 
                 field_connect_char='#',
                 array_connect_char='$',
                 oid_field_name='__oid__',
                 parent_oid_field_name="__parent_oid__",
                 idx_field_name="__idx__",
                 value_field_name="value",
                 get_unique_id=None):
        self.field_connect_char = field_connect_char
        self.array_connect_char = array_connect_char
        self.oid_field_name = oid_field_name
        self.parent_oid_field_name = parent_oid_field_name
        self.idx_field_name = idx_field_name
        self.value_field_name = value_field_name

        if get_unique_id is None:
            self.get_unique_id = lambda : str(uuid.uuid4())
        else:
            self.get_unique_id = get_unique_id
    

    def shim_array_element_schema(self, schema: Schema) -> Schema:    
        # only used for array elements
        # convert a primitive schema to a struct schema
        if schema.is_primitive:
            json_schema = {
                "type": "struct",
                "fields": [
                    {
                        "metadata": {},
                        "name": self.value_field_name,
                        "nullable": True,
                        "type": schema.json_schema
                    },
                ]
            }
            return Schema(json_schema)
        
        if schema.is_array:
            json_schema = {
                "type": "struct",
                "fields": [
                    {
                        "metadata": {},
                        "name": self.value_field_name,
                        "nullable": True,
                        "type": {
                            "containsNull": True,
                            "elementType": self.shim_array_element_schema(schema.elementType).json_schema,
                            "type": "array"
                        }
                    },
                ]
            }
            return Schema(json_schema)

    def flat_schema(self, schema: Schema) -> Schema:
        if schema.is_primitive:
            return schema
        
        if schema.is_array:
            return schema
        
        if schema.is_struct:
            new_json_schema = schema.json_schema
            new_json_schema["fields"] = [] # Going to rebuild fields
            for field in schema.fields:
                if field.type.is_struct:
                    flat_type = self.flat_schema(field.type)
                    for sub_field in flat_type.fields:
                        new_json_field = sub_field.json_field
                        new_json_field["name"] = f"{field.name}{self.field_connect_char}{sub_field.name}"
                        new_json_schema["fields"].append(new_json_field)
                    continue

                new_json_schema["fields"].append(field.json_field)
            
            return Schema(new_json_schema)

        raise Exception(f"Unrecognized type: {schema}")

    def flat_top_schema(self, schema: Schema) -> Dict[str, Schema]:
        assert schema.is_struct

        f_schema = self.flat_schema(schema)
        ret_schema_dict = {}
        
        main_json_schema = f_schema.json_schema
        main_json_schema['fields'] = []
        for field in f_schema.fields:
            if field.type.is_array:
                if field.type.elementType.is_primitive or field.type.elementType.is_array:
                    element_schema = self.shim_array_element_schema(field.type.elementType)
                else:
                    element_schema = field.type.elementType
                element_schema = element_schema.add_field({"metadata": {}, "name": self.parent_oid_field_name, "nullable": True, "type": "string"})
                element_schema = element_schema.add_field({"metadata": {}, "name": self.idx_field_name,    "nullable": True, "type": "long"})
                type_dict = self.flat_top_schema(element_schema)
                for table_name, table_schema in type_dict.items():
                    if table_name == "":
                        ret_schema_dict[field.name] = table_schema
                    else:
                        ret_schema_dict[f"{field.name}{self.array_connect_char}{table_name}"] = table_schema
            else:
                main_json_schema['fields'].append(field.json_field)
        main_schema = Schema(main_json_schema)
        if len(ret_schema_dict) > 0:
            main_schema = main_schema.add_field({"metadata": {}, "name": self.oid_field_name, "nullable": True, "type": "string"})
        ret_schema_dict[""] = main_schema

        return ret_schema_dict

    def _is_primitive_value(self, v):
        return isinstance(v, str) or isinstance(v, int) or isinstance(v, float)

    def flat_value(self, value):
        """retrun a flat value
        """
        if self._is_primitive_value(value):
            return value
        
        if isinstance(value, list):
            return value
        
        if isinstance(value, dict):
            ret_value = {}
            for field_name, field_value in value.items():
                if isinstance(field_value, dict):
                    field_value = self.flat_value(field_value)
                    for sub_field_name, sub_field_value in field_value.items():
                        ret_value[f"{field_name}{self.field_connect_char}{sub_field_name}"] = sub_field_value
                    continue

                ret_value[field_name] = field_value
            return ret_value
        
        raise Exception("Invalid value to flat")

    def shim(self, value):
        # purpose: array element MUST be dict
        if self._is_primitive_value(value):
            return value

        if isinstance(value, dict):
            new_value = {}
            for field_name, field_value in value.items():
                new_value[field_name] = self.shim(field_value)
            return new_value

        if isinstance(value, list):
            new_value = []
            for i in value:
                if self._is_primitive_value(i):
                    new_value.append({self.value_field_name: i})
                    continue
                if isinstance(i, list):
                    new_value.append({self.value_field_name: self.shim(i)})
                    continue
                if isinstance(i, dict):
                    new_value.append(self.shim(i))
                    continue
                raise Exception("bad value")
            return new_value
        
        raise Exception("bad value")

    def flat_top_value(self, value):
        assert isinstance(value, dict)

        value = self.shim(value)

        cloned_value = copy(value)
        cloned_value[self.oid_field_name] = self.get_unique_id()
        pending_table_dict = {
            "": [ cloned_value ]
        }
        ret_table_dict = {
        }

        while len(pending_table_dict) > 0:
            table_dict = pending_table_dict
            pending_table_dict = {}
            for table_name, table_rows in table_dict.items():
                for table_row in table_rows:
                    f_row = self.flat_value(table_row)
                    new_row = {}
                    child_rows_dict = {}
                    for field_name, field_value in f_row.items():
                        if isinstance(field_value, list):
                            for idx, item in enumerate(field_value):
                                if self._is_primitive_value(item) or isinstance(item, list):
                                    raise Exception("should be shimed")
                                else:
                                    effective_item = item
                                    effective_item.update({
                                        self.idx_field_name: idx,
                                        self.parent_oid_field_name: f_row[self.oid_field_name],
                                        self.oid_field_name: self.get_unique_id()
                                    })
                                if field_name not in child_rows_dict:
                                    child_rows_dict[field_name] = []
                                child_rows_dict[field_name].append(effective_item)
                        else:
                            new_row[field_name] = field_value
                    if len(child_rows_dict) == 0:
                        new_row.pop(self.oid_field_name, None)
                    if table_name not in ret_table_dict:
                        ret_table_dict[table_name] = []
                    ret_table_dict[table_name].append(new_row)
                    for child_name, child_rows in child_rows_dict.items():
                        if table_name == "":
                            child_table_name = child_name
                        else:
                            child_table_name = f"{table_name}${child_name}"
                        if child_table_name not in pending_table_dict:
                            pending_table_dict[child_table_name] = []
                        pending_table_dict[child_table_name].extend(child_rows)

        return ret_table_dict

    def flat_top_values(self, values):
        ret_tables = {}

        for value in values:
            tables = self.flat_top_value(value)
            for table_name, table_rows in tables.items():
                if table_name not in ret_tables:
                    ret_tables[table_name] = []
                ret_tables[table_name].extend(table_rows)
        return ret_tables