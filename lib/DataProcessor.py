# DataProcessor.py

from typing import List, Dict, Any, Set
from datetime import datetime, date
import logging
import hashlib
import json
import pytz
from dateutil import parser

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, field_types: Dict[str, str]):
        self.field_types = field_types

    def process_data(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes the data fetched from Airtable.

        Args:
            records (List[Dict[str, Any]]): The raw data from Airtable.

        Returns:
            List[Dict[str, Any]]: The processed data.
        """
        processed = []
        for record in records:
            processed_record = {}
            for key, value in record['fields'].items():
                field_type = self.field_types.get(key, 'singleLineText')  # Default to string if type is unknown
                processed_record[key] = self._convert_value_to_firestore_type(value, field_type)
            processed.append(processed_record)
        
        logger.info(f"Processed {len(processed)} records")
        if processed:
            logger.info(f"Sample record keys: {list(processed[0].keys())}")
        return processed

    def _convert_value_to_firestore_type(self, value: Any, field_type: str) -> Any:
        logger.debug(f"Converting field type {field_type}, input value type: {type(value)}")
        if value is None:
            return None
        if field_type in ('singleLineText', 'multilineText'):
            return str(value)
        elif field_type == 'number':
            return float(value)
        elif field_type == 'checkbox':
            return bool(value)
        elif field_type == 'date':
            result = parser.parse(value).date() if isinstance(value, str) else value
            logger.debug(f"Date conversion result type: {type(result)}, value: {result}")
            return result
        elif field_type == 'dateTime':
            result = parser.parse(value) if isinstance(value, str) else value
            logger.debug(f"DateTime conversion result type: {type(result)}, value: {result}")
            return result
        elif field_type == 'multipleAttachments':
            return [attachment['url'] for attachment in value] if value else []
        elif field_type == 'multipleSelects':
            return list(value)
        else:
            return value  # Return as-is for unsupported types

    def process_duplicate_names(self, data: List[Dict[str, Any]], primary_key: str) -> List[Dict[str, Any]]:
        """
        Processes duplicate names in the data, keeping only the latest record for each name.

        Args:
            data (List[Dict[str, Any]]): The data to process.
            primary_key (str): The primary key field name.

        Returns:
            List[Dict[str, Any]]: The processed data with duplicates removed.
        """
        name_dict = {}
        skipped_count = 0
        for record in data:
            name = record.get(primary_key)
            if name is None or name == '':
                skipped_count += 1
                continue
            
            created_date = self._parse_datetime(record['Created'])
            
            if name in name_dict:
                existing_date = self._parse_datetime(name_dict[name]['Created'])
                if created_date > existing_date:
                    logger.warning(f"Multiple records found with Name '{name}'. Using the record with the latest Created date.")
                    name_dict[name] = record
            else:
                name_dict[name] = record
        
        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} records with undefined Name.")
        
        return list(name_dict.values())

    def calculate_checksum(self, record: Dict[str, Any], fields: Set[str]) -> str:
        """
        Calculates a checksum for the specified fields of a record.

        Args:
            record (Dict[str, Any]): The record to calculate the checksum for.
            fields (Set[str]): The fields to include in the checksum calculation.

        Returns:
            str: The calculated checksum.
        """
        filtered_record = {k: self.normalize_value_for_comparison(record.get(k)) for k in fields if k in record}
        sorted_items = [(k, filtered_record[k]) for k in sorted(filtered_record.keys())]
        record_string = json.dumps(sorted_items, sort_keys=True, default=self._json_serializer).encode('utf-8')
        logger.debug(f"Record string for checksum calculation: {record_string}")
        return hashlib.md5(record_string).hexdigest()

    def _json_serializer(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    def normalize_value_for_comparison(self, value):
        if isinstance(value, str) and self.is_airtable_datetime(value):
            return self.normalize_datetime(parser.parse(value))
        elif isinstance(value, datetime):
            return self.normalize_datetime(value)
        elif isinstance(value, date):
            return value.isoformat()
        return value

    def normalize_datetime(self, dt):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        return dt.astimezone(pytz.UTC).isoformat()

    def is_airtable_datetime(self, value: str) -> bool:
        try:
            parser.parse(value)
            return True
        except ValueError:
            return False

    def _parse_datetime(self, value):
        if isinstance(value, str):
            return parser.parse(value)
        elif isinstance(value, datetime):
            return value
        else:
            raise ValueError(f"Unexpected type for datetime value: {type(value)}")
        
    def calculate_table_checksum(self, table_data: Dict[str, Dict[str, Any]], metadata: Dict[str, str]) -> str:
        """
        Calculates a checksum for the entire table data, including metadata.

        Args:
            table_data (Dict[str, Dict[str, Any]]): The table data with primary keys as outer keys.
            metadata (Dict[str, str]): Metadata including table name and view name.

        Returns:
            str: The calculated checksum.
        """
        data_for_checksum = {
            'metadata': metadata,
            'data': table_data
        }
        
        sorted_table_data = json.dumps(data_for_checksum, sort_keys=True, default=self._json_serializer)
        logger.debug(f"Table data for checksum calculation: {sorted_table_data}")
        return hashlib.md5(sorted_table_data.encode('utf-8')).hexdigest()