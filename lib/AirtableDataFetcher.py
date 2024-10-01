import requests
from typing import List, Dict, Any, Optional
from airtable import Airtable
import logging
from .AirtablePipelineConfigs import AirtableConfig

logger = logging.getLogger(__name__)

class AirtableDataFetcher:
    def __init__(self, config: AirtableConfig):
        self.config = config
        self.airtable = Airtable(config.base_id, config.table_name, api_key=config.api_key)

    def fetch_data(self, view_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetches data from Airtable, either from the entire table or a specific view.

        Args:
            view_name (Optional[str]): The name of the view to fetch data from. If None, fetches from the entire table.

        Returns:
            List[Dict[str, Any]]: A list of records from the Airtable.

        Raises:
            RuntimeError: If there's an error fetching data from Airtable.
        """
        try:
            if view_name:
                data = self.airtable.get_all(view=self.config.view_name)
                logger.info(f"Successfully fetched {len(data)} records from Airtable view '{view_name}'")
            else:
                data = self.airtable.get_all()
                logger.info(f"Successfully fetched {len(data)} records from Airtable table")
            return data
        except Exception as e:
            logger.error(f"Failed to fetch data from Airtable: {e}")
            raise RuntimeError(f"Failed to fetch data from Airtable: {e}")

    def fetch_field_types(self) -> Dict[str, str]:
        """
        Fetches the field types from Airtable.

        Returns:
            Dict[str, str]: A dictionary mapping field names to their types.

        Raises:
            RuntimeError: If there's an error fetching field types from Airtable.
        """
        try:
            url = f"https://api.airtable.com/v0/meta/bases/{self.config.base_id}/tables"
            response = requests.get(url, headers={"Authorization": f"Bearer {self.config.api_key}"})
            response.raise_for_status()
            
            tables = response.json()['tables']
            target_table = next((table for table in tables if table['name'] == self.config.table_name), None)
            
            if not target_table:
                raise ValueError(f"Table '{self.config.table_name}' not found in the Airtable base")
            
            return {field['name']: field['type'] for field in target_table['fields']}
        except Exception as e:
            logger.error(f"Failed to fetch Airtable field types: {e}")
            raise RuntimeError(f"Failed to fetch Airtable field types: {e}")