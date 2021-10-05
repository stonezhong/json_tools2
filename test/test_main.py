import os
import pytest
import json

from json_tools2 import FlatJson, Schema

def load_json_file(filename):
    with open(filename, "rt") as f:
        return json.load(f)

def load_jsonl_file(filename):
    with open(filename, "rt") as f:
        for line in f:
            yield json.loads(line)

def assert_json_equal(a, b, message=""):
    a_str   = json.dumps(a, indent=2, sort_keys=True)
    b_str   = json.dumps(b, indent=2, sort_keys=True)
    assert a_str == b_str, message


def assert_json_list_equal(a, b, message):
    assert len(a) == len(b), f"{message}: both list should have same length"
    for i in range(0, len(a)):
        assert_json_equal(a[i], b[i], f"{message}: i={i}")


def run_test(fj, data_dir):
    json_schema = load_json_file(os.path.join(data_dir, "schema.json"))
    schema = Schema(json_schema)
    flat_schema_dict = fj.flat_top_schema(schema)

    rows = load_jsonl_file(os.path.join(data_dir, "data.json"))
    flat_table_dict = fj.flat_top_values(rows)

    for table_name in os.listdir(os.path.join(data_dir, "flat")):
        if table_name == "__main__":
            effective_table_name = ""
        else:
            effective_table_name = table_name
        if effective_table_name not in flat_schema_dict:
            raise Exception(f"missing table \"{effective_table_name}\"")

        # compare schema
        table_schema = flat_schema_dict.pop(effective_table_name)
        table_schema_exp = load_json_file(os.path.join(data_dir, "flat", table_name, "schema.json"))
        assert_json_equal(
            table_schema.json_schema,
            table_schema_exp,
            f'check schema for table: "{table_name}"'
        )
        
        # compare data
        table_rows = flat_table_dict.pop(effective_table_name)
        table_rows_exp = list(load_jsonl_file(os.path.join(data_dir, "flat", table_name, "data.json")))
        assert_json_list_equal(table_rows, table_rows_exp, f'check table: "{table_name}"')

    assert len(flat_schema_dict)==0, f"following table schema not found: {', '.join(flat_schema_dict.keys())}"
    assert len(flat_table_dict)==0,  f"following table data not found: {', '.join(flat_table_dict.keys())}"
    

class IdGen:
    def __init__(self, init_value=0):
        self.unique_id = init_value

    def __call__(self):
        rv = str(self.unique_id)
        self.unique_id += 1
        return rv
    

class TestFlatJson:
    def test_sample01(self):
        # no nest structure
        # no array field
        fj = FlatJson()
        run_test(fj, "data/sample01")

    def test_sample02(self):
        # has nest structure
        # no array field
        # all fields can be promoted
        fj = FlatJson()
        run_test(fj, "data/sample02")

    def test_sample03(self):
        # has one level of nest array
        fj = FlatJson(get_unique_id=IdGen())
        run_test(fj, "data/sample03")

    def test_sample04(self):
        # has one level of nest array
        fj = FlatJson(get_unique_id=IdGen())
        run_test(fj, "data/sample04")
