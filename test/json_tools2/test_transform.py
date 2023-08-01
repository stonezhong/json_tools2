import pytest

from json_tools2 import json_remove_field, json_traverse_transform, json_convert_to_array
import json
from copy import deepcopy

def test_jrf_simple():
    payload = None
    result = None
    assert json_remove_field(payload, "foo") == result

    payload = "abc"
    result = "abc"
    assert json_remove_field(payload, "foo") == result

    payload = ["a", "b"]
    result = ["a", "b"]
    assert json_remove_field(payload, "foo") == result

    payload = {
        "foo": 1
    }
    result = {}
    assert json_remove_field(payload, "foo") == result

    payload = {
        "foo": 1,
        "bar": 1
    }
    result = {
        "bar": 1
    }
    assert json_remove_field(payload, "foo") == result

    payload = {
        "foo": 1,
        "bar": {
            "foo": 1,
            "x": 2
        }
    }
    result = {
        "bar": {
            "x": 2
        }
    }
    assert json_remove_field(payload, "foo") == result

    payload = {
        "foo": 1,
        "bar": [
            {
                "foo": 1
            },
            {
                "bar": 1
            },
        ]
    }
    result = {
        "bar": [
            {},
            {"bar": 1}
        ]
    }
    assert json_remove_field(payload, "foo") == result

    payload = [
        {
            "foo": 1
        },
        {
            "bar": 2
        }
    ]
    result = [
        {}, {"bar": 2}
    ]
    assert json_remove_field(payload, "foo") == result

def test_json_traverse_transform():
    payload = {
        "foo": 1
    }
    result = {
        "foo": 2
    }
    def handler(v):
        if type(v) != dict or"foo" not in v:
            return
        v["foo"] += 1 
    assert json_traverse_transform(payload, "", handler) == result

    payload = [
        {
            "score": 1
        },
        {
            "score": 2
        },
        {
            "score": 3
        },
    ]
    result = [
        {
            "score": 2
        },
        {
            "score": 3
        },
        {
            "score": 4
        },
    ]
    def handler(v):
        if type(v) != dict or "score" not in v:
            return
        v["score"] += 1 
    
    assert json_traverse_transform(payload, "[]", handler)==result

    payload = [
        {
            "score": {
                "x": 1,
                "y": 2
            }
        },
        {
            "score": {
                "x": 2,
                "y": 3
            }
        },
        {
            "score": {
                "x": 3,
                "y": 4
            }
        },
    ]
    result = [
        {
            "score": {
                "x": 1,
                "y": 2,
                "sum": 3,
            }
        },
        {
            "score": {
                "x": 2,
                "y": 3,
                "sum": 5,
            }
        },
        {
            "score": {
                "x": 3,
                "y": 4,
                "sum": 7,
            }
        },
    ]
    def handler(v):
        if type(v) != dict:
            return
        x = v.get("x", 0)
        y = v.get("y", 0)
        if type(x) in (int, float) and type(y) in (int, float):
            v["sum"] = x + y
    
    assert json_traverse_transform(payload, "[].score", handler)==result

def test_json_convert_to_array():
    payload = [
        {
            "score": {
                "math": 1,
                "physics": 2
            }
        },
        {
            "score": {
                "math": 3,
                "physics": 5
            }
        },
    ]
    result = [
        {
            "score2": [
                {"key": "math", "value": 1},
                {"key": "physics", "value": 2},
            ]
        },
        {
            "score2": [
                {"key": "math", "value": 3},
                {"key": "physics", "value": 5},
            ]
        },
    ]
    assert json_convert_to_array(payload, "score","score2", "[]")==result

    payload = [
        {
            "type": "X",
            "q1": {
                "score": {
                    "math": 1,
                    "physics": 2
                }
            }
        },
        {
            "type": "Y",
            "q1": {
                "score": {
                    "math": 3,
                    "physics": 5
                }
            }
        },
    ]
    result = [
        {
            "type": "X",
            "q1": {
                "score2": [
                    {"key": "math", "value": 1},
                    {"key": "physics", "value": 2},
                ]
            }
        },
        {
            "type": "Y",
            "q1": {
                "score2": [
                    {"key": "math", "value": 3},
                    {"key": "physics", "value": 5},
                ]
            }
        },
    ]
    result2 = [
        {
            "type": "X",
            "q1": {
                "score": [
                    {"key": "math", "value": 1},
                    {"key": "physics", "value": 2},
                ]
            }
        },
        {
            "type": "Y",
            "q1": {
                "score": [
                    {"key": "math", "value": 3},
                    {"key": "physics", "value": 5},
                ]
            }
        },
    ]

    assert json_convert_to_array(deepcopy(payload), "score","score2", "[].q1")==result
    assert json_convert_to_array(deepcopy(payload), "score","score", "[].q1")==result2
