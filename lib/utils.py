import re
from glom import glom, T, SKIP

def exclude_keys(data, keys_to_exclude):
    if isinstance(data, dict):
        return {
            key: exclude_keys(value, keys_to_exclude)
            for key, value in data.items()
            if key not in keys_to_exclude
        }
    elif isinstance(data, list):
        return [exclude_keys(item, keys_to_exclude) for item in data]
    else:
        data = re.sub(r"[\n\t\s]+", " ", data)
        return data
    