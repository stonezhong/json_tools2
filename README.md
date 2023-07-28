# Index
* [Infer Schema](#infer-schema)
* [Schema evolvement](#schema-evolvement)

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
["a", 2]
```
case 2: `"a"` and `2` are of different types and we need to merge schema
```json
[{"foo": "a"}, {"foo": 2}]
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
* You have json object stream
* You can to merge schema for each object from the stream
* You want to save the merged schema, so in case you need, to can load schema saved and continue to merge more objects from the stream.

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