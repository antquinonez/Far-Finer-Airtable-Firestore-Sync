import re
import json
import csv

from glom import glom, T, SKIP

def exclude_keys(data, keys_to_exclude):
    """Recursively removes specified keys from a dictionary or list.
    
    Args:
        data: The data structure to process (dict, list, or primitive type)
        keys_to_exclude (list): List of keys to exclude from dictionaries
        
    Returns:
        The data structure with the specified keys removed.
    """
    if isinstance(data, dict):
        return {
            key: exclude_keys(value, keys_to_exclude) 
            for key, value in data.items() 
            if key not in keys_to_exclude
        }
    elif isinstance(data, list):
        return [exclude_keys(item, keys_to_exclude) for item in data]
    elif isinstance(data, str):
        # Only process strings with regex
        import re
        return re.sub(r"[\t]+", "", data).strip()
    else:
        # Return all other types (int, float, bool, etc.) as-is
        return data


def write_dict_to_csv(json_string, filename, priority_fields=None):
    # Parse the JSON string into a Python dictionary
    data = json.loads(json_string)

    # Check if the data is empty
    if not data:
        print("The data dictionary is empty.")
        return

    # Get all possible fieldnames from all nested dictionaries
    fieldnames = set()
    for nested_dict in data.values():
        fieldnames.update(nested_dict.keys())
    
    # Sort the fieldnames
    if priority_fields:
        # Ensure all priority fields are actually in the data
        priority_fields = [field for field in priority_fields if field in fieldnames]
        other_fields = sorted(field for field in fieldnames if field not in priority_fields)
        fieldnames = priority_fields + other_fields
    else:
        fieldnames = sorted(list(fieldnames))

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        
        # Write the header
        writer.writeheader()
        
        # Write the rows
        for nested_dict in data.values():
            writer.writerow(nested_dict)

    print(f"CSV file '{filename}' has been created successfully.")
