import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from airtable import Airtable
from google.cloud import datastore

from .AirtableToDatastore import AirtableToDatastore
from .AirtablePipelineConfigs import AirtableConfig, DatastoreConfig, PipelineConfig, UpdateType

logger = logging.getLogger(__name__)

class AirtableToDatastoreBuilder:
    def __init__(self):
        self._airtable_config = None
        self._datastore_config = None
        self._primary_key = None
        self._update_type = None

    def with_airtable_config(self, base_id: str, table_name: str, view_name: Optional[str] = None, api_key: Optional[str] = None) -> 'AirtableToDatastoreBuilder':
        self._airtable_config = AirtableConfig(base_id, table_name, view_name, api_key)
        return self

    def with_datastore_config(self, project_id: str, kind: str, database_id: str) -> 'AirtableToDatastoreBuilder':
        self._datastore_config = DatastoreConfig(project_id, kind, database_id)
        return self
    
    def with_primary_key(self, primary_key: str) -> 'AirtableToDatastoreBuilder':
        self._primary_key = primary_key
        return self

    def with_update_type(self, update_type: UpdateType) -> 'AirtableToDatastoreBuilder':
        self._update_type = update_type
        return self

    def build(self) -> 'AirtableToDatastore':
        if not all([self._airtable_config, self._datastore_config, self._primary_key, self._update_type]):
            raise ValueError("All configurations must be set before building")
        
        config = PipelineConfig(self._airtable_config, self._datastore_config, self._primary_key, self._update_type)
        return AirtableToDatastore(config)


# def main():
#     # Example usage with the builder
#     pipeline = (AirtableToDatastoreBuilder()
#                 .with_airtable_config('your_base_id', 'your_table_name')
#                 .with_datastore_config('your_project_id', 'your_datastore_kind')
#                 .with_primary_key('Name')
#                 .build())
#     pipeline.run_pipeline()

# if __name__ == "__main__":
#     main()