from typing import Optional, Any, List, Callable
from abc import ABC, abstractmethod

################################################################################################################
# json_traverse_transform       transform an object with a path pattern
# json_remove_field             remove a field recursively
# json_convert_to_array         convert field to array
################################################################################################################

class Accessor(ABC):
    @abstractmethod
    def access(self, obj:Any)-> List[Any]:
        pass


class ObjFieldAccessor(Accessor):
    field_name:str

    def __init__(self, field_name:str):
        self.field_name = field_name

    def __str__(self) ->str :
        return f"ObjFieldAccessor(field_name={self.field_name})"
    
    def access(self, obj:Any) -> List[Any]:
        if type(obj) != dict:
            return []

        if self.field_name == "*":
            return [field_value for _, field_value in obj.items()]

        if self.field_name not in obj:
            return []
           
        return [ obj[self.field_name] ] if self.field_name in obj else []


class ListElementAccessor(Accessor):
    def __str__(self) -> str:
        return f"ListElementAccessor()"

    def access(self, obj:Any) -> Any:
        if type(obj) != list:
            return []
        return obj

# Valid path are
# Given a jsob object X, here is what the path represents
# "foo.bar"        --> [X.foo.bar]
# "foo.bar[]"      --> [X.foo.bar[0], X.foo.bar[1], ...]
# "foo.bar[].x"    --> [X.foo.bar[0].x, X.foo.bar[1].x, ...]
# "foo.bar[][]"    --> [X.foo.bar[0][0], X.foo.bar[0][1], X.foo.bar[1][0], X.foo.bar[1][1], ...]
def accessors_from_path(path:str) -> List[Accessor]:
    ret = []
    segs = path.split(".")
    for seg in segs:
        idx_start = seg.find("[")
        if idx_start == -1:
            field_name = seg
            idx_str = ""
        else:
            field_name = seg[:idx_start]
            idx_str = seg[idx_start:]
        if field_name:
            ret.append(ObjFieldAccessor(field_name))
        depth = len(idx_str)//2
        assert idx_str == "[]"*depth
        for _ in range(depth):
            ret.append(ListElementAccessor())
    return ret


def _json_traverse(obj:Any, accessors:List[Accessor]) -> List[Any]:
    q = [obj]
    for accessor in accessors:
        new_objs = []
        for o in q:
            r = accessor.access(o)
            new_objs.extend(r)
        q = new_objs
    
    return q

def json_traverse_transform(obj: Any, path:str, handler:Callable[[Any], None]) -> Any:
    # Traverse an object
    # path: the path pattern
    # handler: handler that is called when path matches
    # return: the object mutated
    accessors = accessors_from_path(path)
    objs = _json_traverse(obj, accessors)
    for o in objs:
        handler(o)
    return obj


def json_remove_field(obj:Any, field_name:str) -> Any:
    # obj: a json object
    # field_name: name of the field to remove
    # remove a field recursively, normally for removing PII
    if isinstance(obj, dict):
        return {k:json_remove_field(v, field_name) for k, v in obj.items() if k != field_name}
    if isinstance(obj, list):
        return [json_remove_field(element, field_name) for element in obj]
    return obj


# convert a dict to array of key/value pair
# we create a new field and put array to it
# and delete the original field to avoid schema explosion
def json_convert_to_array(obj:Any, field_name:str, new_field_name:str, path="")->Any:
    def handler(obj: Any) -> Any:
        v = obj.get(field_name)
        if not isinstance(v, dict):
            return obj
        del obj[field_name]
        obj[new_field_name] = [{"key":k, "value":v} for k, v in v.items()]
        return obj
    
    return json_traverse_transform(obj, path, handler)
