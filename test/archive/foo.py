#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
from pyspark.sql import SparkSession

from flat_json import Schema, Field, FlatJson


def f_to_rows(f):
    for line in f:
        yield json.loads(line)

unique_id = {
    "value": 0
}

def get_unique_id():
    rv = str(unique_id["value"])
    unique_id["value"] += 1
    return rv

def test(spark):
    fj = FlatJson(get_unique_id=get_unique_id)
    json_location = "data/sample04/data.json"
    df = spark.read.json(json_location)
    json_schema = json.loads(df.schema.json())

    schema = Schema(json_schema)
    # print(schema)
    # return
    # print(schema.strfmt())
    # print("-----------------")
    schema_dict = fj.flat_top_schema(schema)
    for table_name, table_schema in schema_dict.items():
        print(f"table name: {table_name}")
        # print(table_schema.strfmt())
        print(table_schema)

    with open(json_location, "rt") as jf:
        rows = f_to_rows(jf)
        table_dict = fj.flat_top_values(rows)
        for table_name, table_rows in table_dict.items():
            print(f"table: {table_name}")
            for row in table_rows:
                print(json.dumps(row))


    

def main():
    spark = SparkSession.builder.appName("test-flat").getOrCreate()
    try:
        test(spark)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()