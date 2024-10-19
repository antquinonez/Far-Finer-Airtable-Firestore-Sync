from dataclasses import dataclass
from typing import Optional
from enum import Enum

class UpdateType(Enum):
    FULL_REFRESH = "full_refresh"
    REPLACE = "replace"
    VERSIONED = "versioned"
    UPSERT_CHECKSUM = "upsert_checksum"
    SOFT_DELETE = "soft_delete"
    UPSERT_CHECKSUM_WITH_DELETE = "upsert_checksum_with_delete"
    VERSIONED_SET = "versioned_set"
    VERSIONED_CHECKSUM = "versioned_checksum" 
    VERSIONED_TABLE_CHECKSUM = "versioned_table_checksum"
    UPSERT_TABLE_CHECKSUM = "upsert_table_checksum" 

@dataclass
class AirtableConfig:
    base_id: str
    table_name: str
    view_name: Optional[str] = None
    api_key: Optional[str] = None

    def validate(self):
        if not all([self.base_id, self.table_name, self.api_key]):
            raise ValueError("All Airtable configurations must be set")

@dataclass
class DatastoreConfig:
    project_id: str
    database_id: str 
    kind: str

    def validate(self):
        if not all([self.project_id, self.database_id, self.kind]):
            raise ValueError("All Datastore configurations must be set")

@dataclass
class PipelineConfig:
    airtable: AirtableConfig
    datastore: DatastoreConfig
    primary_key: str
    update_type: UpdateType

    def validate(self):
        if not all([self.airtable, self.datastore, self.primary_key, self.update_type]):
            raise ValueError("All configurations must be set")
        self.airtable.validate()
        self.datastore.validate()