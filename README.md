# Index
* [Infer Schema](#infer-schema)
* [Schema evolvement](#schema-evolvement)
* [JSON Transform](#json-transform)
    * [json_remove_field](#json_remove_field)
    * [json_traverse_transform](#json_traverse_transform)
    * [json_convert_to_array](#json_convert_to_array)

# Infer Schema

With function `infer_schema`, you can get schema from json object. The output complys to [JSON Schema Specification](https://json-schema.org/specification.html). Here is an example:

```python
#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from json_tools2 import infer_schema
import json

def main():
    schema = infer_schema({"foo": 1})
    print(json.dumps(schema, indent=4))

if __name__ == '__main__':
    main()
```
Output
```json
{
    "type": "object",
    "properties": {
        "foo": {
            "type": "integer"
        }
    }
}
```
---
When you have an array of element from different types, or object with the same property but from different type, we need to merge schema.

case 1: `"a"` and `2` are of different types and we need to merge schema
```json
[2.1, 2]
```
case 2: `2.1` and `2` are of different types and we need to merge schema
```json
[{"foo": 2.1}, {"foo": 2}]
```

Following rules applies when we do schema merging:
* For `null`, any type can merged to it and return a `nullable type`, for example, `sting` merge to `null` and return a `nullable string`
* For `string`, only `null` can merged to `string` and return `nullable string`.
* For `number`, followings are the only valid type that can merge to it
    * `integer` can merged to it without changing it
    * `null` can merge to it and become `nullable number`
* For `integer`, followings are the only valid type that can merge to it
    * `number` can merged to it and upgrade it to `number`
    * `null` can merge to it and become `nullable number`
* For `boolean`, followings are the only valid type that can merge to it
    * `null` can merge to it and become `nullable boolean`
* For `object`, followings are the only valid type that can merge to it
    * `null` can merge to it and become `nullable object`
* For `array`, followings are the only valid type that can merge to it
    * `null` can merge to it and become `nullable array`


We use `anyOf` to represent `nullable type`, for example:
This represent a `nullable integer` type.
```json
{
    "anyOf": [{"type": "integer"},{"type": "null"}]
}
```

For any schema inferred, you should be able to validate the payload with the schema inferred, here is an example:
```python
#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# You need to do "pip install jsonschema"
from json_tools2 import infer_schema
from jsonschema import validate
import json


def main():
    payload = {"foo": 1}
    schema = infer_schema(payload)
    validate(instance=payload, schema=schema) # it should not throw exception

if __name__ == '__main__':
    main()

```

# Schema evolvement

Think about such case:
* You receive json object every day.
* Every day, you want to load schema inferred from the json object from the prior day, and merge it with the schema inferred from today's object, and then save the inferred schema.

Here is an example:
```python
#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from json_tools2 import get_schema, ValueSchema
import json

def main():
    schema = get_schema(1)
    print(json.dumps(schema.to_json(), indent=4))
    saved_schema = schema.dump_json()
    # You got a json object saved_schema which you can save the inferred schema

    # load schema
    schema = ValueSchema.load_json(saved_schema)
    schema.merge_from(get_schema(None))
    print(json.dumps(schema.to_json(), indent=4))


if __name__ == '__main__':
    main()
```
Output
```
{
    "type": "integer"
}
{
    "anyOf": [
        {
            "type": "integer"
        },
        {
            "type": "null"
        }
    ]
}
```

# JSON Transform
## json_remove_field

```python
json_remove_field(obj:Any, field_name:str) -> Any:
```
It removes field specified in `field_name` recursively.

Example:
```python
json_remove_field({
    "foo": 1, 
    "bar": {"foo": 1, "t": 2}
}, )
```
it will remove field "foo" recursively, so it will return
```json
{
    "bar": {
        "t": 2
    }
}
```

## json_traverse_transform
```
json_traverse_transform(obj: Any, path:str, handler:Callable[[Any], None]) -> Any:
```
It traverse the input json object `obj`, for each object that match the path pattern specified in `path`, it calls the function `handler`. Eventually `obj` is retuened however, it might be mutated by `handler`.

Path pattern:
```
Given object, a path represent a list of object extracted from object.

For example, given object X, here are example pathes:

"foo"        represent X.foo
"foo.bar"    represent X.foo.bar
"foo[]"      represent [X.foo[0], X.foo[1], ...]
"foo[].x"    represent [X.foo[0].x, X.foo[1].x, ...]
"foo[].x[]"  represent [X.foo[0].x[0], X.foo[0].x[1], ..., X.foo[1].x[0], X.foo[1].x[1], ...]
```

Example:
```python
def handler(o):
    if type(o) != dict:
        return o
    x = o.get("x", 0)
    y = o.get("y", 0)
    if type(x) in (int, float) and type(y) in (int, float):
        o["sum"] = x+y

json_traverse_transform(
    {
        "cords": [
            {"x": 1, "y": 2},
            {"x": 2, "y": 3},
        ]
    }, "cords[]", handler
)
```

returns
```json
{
    "cords": [
        {"x": 1, "y": 2, "sum": 3},
        {"x": 2, "y": 3, "sum": 5},
    ]
}
```

## json_convert_to_array
```python
json_convert_to_array(obj:Any, field_name:str, new_field_name:str, path="")->Any:
```
For each matching child object based on pattern path, it convert it's field `field_name` from dictionary into `list`, result is stored in field `new_field_name`, the original field is either deleted or overritten.

Example:
```python
json_convert_to_array([
    {
        "q1": {
            "score": {"math": 1, "physics": 2}
        }
    },
    {
        "q1": {
            "score": {"math": 2, "physics": 3}
        }
    }
], "score", "score2", "[].q1")
```
returns
```json
[
    {
        "q1": {
            "score": [
                {"key": "math", "value": 1},
                {"key": "physics", "value": 2}
            ]
        }
    },
    {
        "q1": {
            "score": [
                {"key": "math", "value": 2},
                {"key": "physics", "value": 3}
            ]
        }
    },
]
```