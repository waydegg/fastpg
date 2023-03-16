import json
import re
from enum import Enum
from typing import Any, List
from uuid import UUID

from ipdb import set_trace


def compile_value(value):
    if isinstance(value, dict):
        return json.dumps(value)
    elif isinstance(value, UUID):
        return str(value)
    elif isinstance(value, Enum):
        return value.value
    else:
        return value


def compile_query(query: str, values: dict | List[dict] | None = None):

    if type(values) is None:
        return query, tuple()

    compiled_query = query
    ordered_values = []

    if type(values) is dict:
        for i, (k, v) in enumerate(values.items(), start=1):
            compiled_query = re.sub(f":{k}(?![_a-zA-Z0-9])", f"${i}", compiled_query)
            ordered_values.append(compile_value(v))

    if type(values) is list:
        ordered_values.append([])
        for i, (k, v) in enumerate(values[0].items(), start=1):
            compiled_query = re.sub(f":{k}(?![_a-zA-Z0-9])", f"${i}", compiled_query)
            ordered_values[0].append(compile_value(v))
        if len(values) > 1:
            for value_set in values[1:]:
                ordered_value_set = []
                for base_k in values[0].keys():
                    ordered_value_set.append(compile_value(value_set[base_k]))
                ordered_values.append(ordered_value_set)

    return compiled_query, ordered_values
