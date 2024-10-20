import re
from glom import glom, T, SKIP

def exclude_keys(data, keys_to_exclude):
    """Recursively removes specified keys from a dictionary or list.

    Args:
        data (dict or list): The data structure to process.
        keys_to_exclude (list): A list of keys to exclude.

    Returns:
        dict or list: The data structure with the specified keys removed.
    """
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
    