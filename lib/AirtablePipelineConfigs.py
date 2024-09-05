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

@dataclass
class AirtableConfig:
    base_id: str
    table_name: str
    api_key: Optional[str] = None

@dataclass
class DatastoreConfig:
    project_id: str
    database_id: str 
    kind: str

@dataclass
class PipelineConfig:
    airtable: AirtableConfig
    datastore: DatastoreConfig
    primary_key: str
    update_type: UpdateType