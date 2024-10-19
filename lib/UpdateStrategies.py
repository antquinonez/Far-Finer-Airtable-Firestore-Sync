import time
import logging
import pytz
import json
import hashlib
import os
import requests

from abc import ABC, abstractmethod
from typing import Set, Tuple, List, Dict, Any
from datetime import datetime

from pyairtable import Api
from pyairtable import Table as pyAirtableTable

from google.cloud import firestore

from .AirtablePipelineConfigs import PipelineConfig, UpdateType, AirtableConfig
from .FirestoreWrapper import FirestoreWrapper
from .DataProcessor import DataProcessor

logger = logging.getLogger(__name__)

# Set the version ID to the current timestamp
version_id = int(datetime.now(pytz.UTC).timestamp())

class BaseUpdateStrategy(ABC):
    def update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        logger.info(f"Starting {self.__class__.__name__} update")
        
        existing_docs = firestore_wrapper.query_documents_not_equal('update_type', config.update_type.value)
        for doc in existing_docs:
            firestore_wrapper.delete_document(doc['id'])
        logger.info(f"Deleted {len(existing_docs)} documents with different update_type")

        self._perform_update(firestore_wrapper, data, config, data_processor)

        logger.info(f"Completed {self.__class__.__name__} update of {len(data)} records in Firestore")

    @abstractmethod
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        pass

class FullRefreshStrategy(BaseUpdateStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        firestore_wrapper.clear_collection()

        for record in data:
            new_record = {
                **record,
                'update_type': config.update_type.value,
                'write_timestamp': firestore.SERVER_TIMESTAMP
            }
            logger.debug(f"Adding document, data types: {[(k, type(v)) for k, v in new_record.items()]}")
            firestore_wrapper.add_document(new_record)

class ReplaceStrategy(BaseUpdateStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        for record in data:
            primary_key_value = record[config.primary_key]
            
            existing_docs = firestore_wrapper.query_documents(config.primary_key, '==', primary_key_value)
            for doc in existing_docs:
                firestore_wrapper.delete_document(doc['id'])

            new_record = {
                **record,
                'update_type': config.update_type.value,
                'write_timestamp': firestore.SERVER_TIMESTAMP
            }
            logger.debug(f"Adding document, data types: {[(k, type(v)) for k, v in new_record.items()]}")
            firestore_wrapper.add_document(new_record)

class SoftDeleteStrategy(BaseUpdateStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        processed_records = set()

        for record in data:
            primary_key_value = record[config.primary_key]
            processed_records.add(primary_key_value)
            
            existing_docs = firestore_wrapper.query_documents(config.primary_key, '==', primary_key_value)
            
            if existing_docs:
                existing_doc = existing_docs[0]
                new_checksum = data_processor.calculate_checksum(record, set(record.keys()))
                existing_checksum = data_processor.calculate_checksum(existing_doc, set(record.keys()))
                
                if new_checksum != existing_checksum or existing_doc.get('is_deleted', False):
                    update_data = {
                        **record,
                        'update_type': config.update_type.value,
                        'write_timestamp': firestore.SERVER_TIMESTAMP,
                        'is_deleted': False
                    }
                    logger.debug(f"Updating document, data types: {[(k, type(v)) for k, v in update_data.items()]}")
                    firestore_wrapper.update_document(existing_doc['id'], update_data)
                    logger.info(f"Updated document with {config.primary_key}: {primary_key_value}")
                else:
                    logger.debug(f"No changes detected for document with {config.primary_key}: {primary_key_value}. Skipping update.")
            else:
                new_record = {
                    **record,
                    'update_type': config.update_type.value,
                    'write_timestamp': firestore.SERVER_TIMESTAMP,
                    'is_deleted': False
                }
                logger.debug(f"Adding new document, data types: {[(k, type(v)) for k, v in new_record.items()]}")
                firestore_wrapper.add_document(new_record)
                logger.info(f"Inserted new document with {config.primary_key}: {primary_key_value}")

        all_docs = firestore_wrapper.query_documents('update_type', '==', config.update_type.value)
        for doc in all_docs:
            if doc[config.primary_key] not in processed_records and not doc.get('is_deleted', False):
                update_data = {'is_deleted': True, 'write_timestamp': firestore.SERVER_TIMESTAMP}
                logger.debug(f"Marking document as deleted, data types: {[(k, type(v)) for k, v in update_data.items()]}")
                firestore_wrapper.update_document(doc['id'], update_data)
                logger.info(f"Marked document as deleted with {config.primary_key}: {doc[config.primary_key]}")


class VersionedStrategy(BaseUpdateStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        existing_docs = firestore_wrapper.query_documents('update_type', '==', config.update_type.value)
        for doc in existing_docs:
            logger.debug(f"Setting 'latest' to False for document: {doc['id']}")
            firestore_wrapper.update_document(doc['id'], {'latest': False})
        
        for record in data:
            new_record = {
                **record,
                'update_type': config.update_type.value,
                'write_timestamp': firestore.SERVER_TIMESTAMP,
                'version': version_id,
                'start_date': firestore.SERVER_TIMESTAMP,
                'end_date': None,
                'latest': True
            }
            logger.debug(f"Adding new versioned document, data types: {[(k, type(v)) for k, v in new_record.items()]}")
            firestore_wrapper.add_document(new_record)

class VersionedChecksumStrategy(BaseUpdateStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        for record in data:
            source_fields = set(record.keys())
            primary_key_value = record[config.primary_key]
            
            # Query all existing versions of the document
            existing_docs = firestore_wrapper.query_documents(config.primary_key, '==', primary_key_value)
            
            # Sort existing documents by version_id in descending order
            existing_docs.sort(key=lambda x: x.get('version_id', 0), reverse=True)
            
            new_checksum = data_processor.calculate_checksum(record, source_fields)
            
            if existing_docs:
                latest_doc = existing_docs[0]
                existing_checksum = data_processor.calculate_checksum(latest_doc, source_fields)
                
                if new_checksum != existing_checksum:
                    self._create_new_version(firestore_wrapper, record, config, existing_docs)
                else:
                    logger.debug(f"No changes detected for document with {config.primary_key}: {primary_key_value}. Skipping update.")
            else:
                self._create_new_version(firestore_wrapper, record, config, [])
            
    def _create_new_version(self, firestore_wrapper: FirestoreWrapper, record: Dict[str, Any], config: PipelineConfig, existing_docs: List[Dict[str, Any]]) -> None:
        new_version = {
            **record,
            'update_type': config.update_type.value,
            'write_timestamp': firestore.SERVER_TIMESTAMP,
            'version_id': version_id,
            'latest': True
        }
        
        # Create the new version
        new_doc_id = firestore_wrapper.add_document(new_version)
        logger.info(f"Created new version of document with {config.primary_key}: {record[config.primary_key]}")
        
        # Update existing versions
        batch_operations = []
        for doc in existing_docs:
            if doc.get('latest', False):
                batch_operations.append({
                    'operation': 'update',
                    'doc_id': doc['id'],
                    'data': {'latest': False}
                })
        
        if batch_operations:
            firestore_wrapper.batch_write(batch_operations)
            logger.info(f"Updated 'latest' flag for {len(batch_operations)} existing versions")

    def update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        logger.info(f"Starting {self.__class__.__name__} update")
        
        self._perform_update(firestore_wrapper, data, config, data_processor)
        
        # Handle deletions (documents in Firestore that are not in the new data)
        existing_docs = firestore_wrapper.query_documents('update_type', '==', config.update_type.value)
        existing_keys = set(doc[config.primary_key] for doc in existing_docs)
        new_keys = set(record[config.primary_key] for record in data)
        
        # keys_to_delete = existing_keys - new_keys
        
        # for key in keys_to_delete:
        #     docs_to_delete = firestore_wrapper.query_documents(config.primary_key, '==', key)
        #     for doc in docs_to_delete:
        #         firestore_wrapper.delete_document(doc['id'])
        #         logger.info(f"Deleted document with {config.primary_key}: {key}")
        
        logger.info(f"Completed {self.__class__.__name__} update of {len(data)} records in Firestore")


class UpsertChecksumStrategy(BaseUpdateStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        for record in data:
            source_fields = set(record.keys())
            primary_key_value = record[config.primary_key]
            
            existing_docs = firestore_wrapper.query_documents(config.primary_key, '==', primary_key_value)
            
            if existing_docs:
                existing_doc = existing_docs[0]
                new_checksum = data_processor.calculate_checksum(record, source_fields)
                existing_checksum = data_processor.calculate_checksum(existing_doc, source_fields)
                
                if new_checksum != existing_checksum:
                    update_data = {
                        **record,
                        'update_type': config.update_type.value,
                        'version_id': version_id,
                        'write_timestamp': firestore.SERVER_TIMESTAMP
                    }
                    logger.debug(f"Updating document, data types: {[(k, type(v)) for k, v in update_data.items()]}")
                    firestore_wrapper.update_document(existing_doc['id'], update_data)
                    logger.info(f"Updated document with {config.primary_key}: {primary_key_value}")
                else:
                    logger.debug(f"No changes detected for document with {config.primary_key}: {primary_key_value}. Skipping update.")
            else:
                new_record = {
                    **record,
                    'update_type': config.update_type.value,
                    'version_id': version_id,
                    'write_timestamp': firestore.SERVER_TIMESTAMP
                }
                logger.debug(f"Adding new document, data types: {[(k, type(v)) for k, v in new_record.items()]}")
                firestore_wrapper.add_document(new_record)
                logger.info(f"Inserted new document with {config.primary_key}: {primary_key_value}")

class UpsertChecksumWithDeleteStrategy(BaseUpdateStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        processed_records = set()

        for record in data:
            source_fields = set(record.keys())
            primary_key_value = record[config.primary_key]
            processed_records.add(primary_key_value)
            
            existing_docs = firestore_wrapper.query_documents(config.primary_key, '==', primary_key_value)
            
            if existing_docs:
                existing_doc = existing_docs[0]
                new_checksum = data_processor.calculate_checksum(record, source_fields)
                existing_checksum = data_processor.calculate_checksum(existing_doc, source_fields)
                
                if new_checksum != existing_checksum:
                    update_data = {
                        **record,
                        'update_type': config.update_type.value,
                        'version_id': version_id,
                        'write_timestamp': firestore.SERVER_TIMESTAMP
                    }
                    logger.debug(f"Updating document, data types: {[(k, type(v)) for k, v in update_data.items()]}")
                    firestore_wrapper.update_document(existing_doc['id'], update_data)
                    logger.info(f"Updated document with {config.primary_key}: {primary_key_value}")
                else:
                    logger.debug(f"No changes detected for document with {config.primary_key}: {primary_key_value}. Skipping update.")
            else:
                new_record = {
                    **record,
                    'update_type': config.update_type.value,
                    'version_id': version_id,
                    'write_timestamp': firestore.SERVER_TIMESTAMP
                }
                logger.debug(f"Adding new document, data types: {[(k, type(v)) for k, v in new_record.items()]}")
                firestore_wrapper.add_document(new_record)
                logger.info(f"Inserted new document with {config.primary_key}: {primary_key_value}")

        all_docs = firestore_wrapper.query_documents('update_type', '==', config.update_type.value)
        for doc in all_docs:
            if doc[config.primary_key] not in processed_records:
                logger.debug(f"Deleting document: {doc['id']}")
                firestore_wrapper.delete_document(doc['id'])
                logger.info(f"Deleted document with {config.primary_key}: {doc[config.primary_key]}")

class VersionedSetStrategy(BaseUpdateStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        
        existing_docs = firestore_wrapper.query_documents('latest', '==', True)
        existing_data = {doc[config.primary_key]: doc for doc in existing_docs}
        
        changes_detected = self._detect_changes(data, existing_data, config.primary_key, data_processor)
        
        if changes_detected:
            batch_operations = [
                {'operation': 'update', 'doc_id': doc['id'], 'data': {'latest': False}}
                for doc in existing_docs
            ]
            logger.debug(f"Batch updating {len(batch_operations)} documents to set 'latest' to False")
            firestore_wrapper.batch_write(batch_operations)
            
            batch_operations = [
                {'operation': 'create', 'data': {
                    **record,
                    'update_type': config.update_type.value,
                    'write_timestamp': firestore.SERVER_TIMESTAMP,
                    'version_id': version_id,
                    'latest': True
                }}
                for record in data
            ]
            logger.debug(f"Batch creating {len(batch_operations)} new versioned documents, sample data types: {[(k, type(v)) for k, v in batch_operations[0]['data'].items() if k != 'update_type']}")
            firestore_wrapper.batch_write(batch_operations)
            
            logger.info(f"Created new versioned set with version_id: {version_id}, updated {len(data)} records. Changes were detected.")
        else:
            logger.info(f"No changes detected. Skipping update. Existing version remains current.")

    def _detect_changes(self, new_data: List[Dict[str, Any]], existing_data: Dict[str, Dict[str, Any]], primary_key: str, data_processor: DataProcessor) -> bool:
        for record in new_data:
            primary_key_value = record[primary_key]
            if primary_key_value in existing_data:
                if data_processor.calculate_checksum(record, set(record.keys())) != data_processor.calculate_checksum(existing_data[primary_key_value], set(record.keys())):
                    logger.debug(f"Change detected for record with {primary_key}: {primary_key_value}")
                    return True
            else:
                logger.debug(f"New record detected with {primary_key}: {primary_key_value}")
                return True
        
        for existing_key in existing_data:
            if existing_key not in [record[primary_key] for record in new_data]:
                logger.debug(f"Deleted record detected with {primary_key}: {existing_key}")
                return True
        
        return False

# =============================================================================================================================================================
# TABLES
# =============================================================================================================================================================

# TODO: make view optional. if not passed, then use full table
class BaseTableChecksumStrategy(BaseUpdateStrategy, ABC):
    def _filter_data_by_view(self, airtable_config: AirtableConfig) -> Tuple[List[Dict[str, Any]], Set[str]]:
        api_key = os.environ.get('AIRTABLE_API_KEY') or airtable_config.api_key
        if not api_key:
            raise ValueError("Airtable API key is not set")

        table = pyAirtableTable(api_key, airtable_config.base_id, airtable_config.table_name)
        
        try:
            view_metadata = self._get_view_metadata(airtable_config, api_key)
            view_fields = set(view_metadata['fields'])
            logger.info(f"Fields visible in the view according to metadata: {view_fields}")
            
            view_data = table.all(view=airtable_config.view_name, fields=list(view_fields))
            logger.info(f"Successfully fetched {len(view_data)} records from view '{airtable_config.view_name}'")
            
            if view_data:
                actual_fields = set(view_data[0]['fields'].keys())
                logger.info(f"Fields actually present in fetched data: {actual_fields}")
                if actual_fields != view_fields:
                    logger.warning(f"Mismatch between expected and actual fields. Difference: {actual_fields.symmetric_difference(view_fields)}")
            
            return [record['fields'] for record in view_data], view_fields
        except Exception as e:
            logger.error(f"Error fetching data from Airtable view: {e}")
            raise RuntimeError(f"Failed to fetch data from Airtable view: {e}")

    def _get_view_metadata(self, airtable_config: AirtableConfig, api_key: str) -> Dict[str, Any]:
        url = f"https://api.airtable.com/v0/meta/bases/{airtable_config.base_id}/tables"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        tables = response.json()['tables']
        target_table = next((table for table in tables if table['name'] == airtable_config.table_name), None)
        
        if not target_table:
            raise ValueError(f"Table '{airtable_config.table_name}' not found in the Airtable base")
        
        target_view = next((view for view in target_table['views'] if view['name'] == airtable_config.view_name), None)
        
        if not target_view:
            raise ValueError(f"View '{airtable_config.view_name}' not found in the table '{airtable_config.table_name}'")
        
        all_fields = {field['id']: field['name'] for field in target_table['fields']}
        visible_fields = self._get_visible_fields(target_view, all_fields)
        
        return {
            'id': target_view['id'],
            'name': target_view['name'],
            'fields': visible_fields
        }

    def _get_visible_fields(self, view: Dict[str, Any], all_fields: Dict[str, str]) -> List[str]:
        if 'visibleFieldOrder' in view:
            visible_fields = [all_fields[field_id] for field_id in view['visibleFieldOrder'] if field_id in all_fields]
        else:
            visible_fields = list(all_fields.values())
        
        if 'hiddenFields' in view:
            hidden_fields = set(view['hiddenFields'])
            visible_fields = [field for field in visible_fields if field not in hidden_fields]
        
        return visible_fields

    def _prepare_table_data(self, config: PipelineConfig, data_processor: DataProcessor) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], str]:
        table_name = config.airtable.table_name
        view_name = config.airtable.view_name

        metadata = {
            'Name': table_name,
            'ViewName': view_name
        }

        view_data, view_fields = self._filter_data_by_view(config.airtable)
        
        sample_record = view_data[0] if view_data else {}
        logger.info(f"Fields present in fetched data: {set(sample_record.keys())}")
        
        view_table_data = {
            record[config.primary_key]: {k: v for k, v in record.items() if k in view_fields}
            for record in view_data if config.primary_key in record
        }
        
        sample_processed_record = next(iter(view_table_data.values())) if view_table_data else {}
        logger.info(f"Fields in processed view_table_data: {set(sample_processed_record.keys())}")

        table_checksum = data_processor.calculate_table_checksum(view_table_data, metadata)
        
        return view_table_data, metadata, table_checksum

    @abstractmethod
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        pass


class VersionedTableChecksumStrategy(BaseTableChecksumStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        view_table_data, metadata, table_checksum = self._prepare_table_data(config, data_processor)
        
        existing_versions = firestore_wrapper.query_all_versions(config.update_type.value)
        
        if existing_versions and existing_versions[0].get('table_checksum') == table_checksum:
            logger.info(f"No changes detected for table '{metadata['Name']}' with view '{metadata['ViewName']}'. Skipping update.")
            return

        new_version = {
            'update_type': config.update_type.value,
            'write_timestamp': firestore.SERVER_TIMESTAMP,
            'version_id': version_id,
            'table_checksum': table_checksum,
            'table_data': view_table_data,
            'metadata': metadata,
            'latest': True
        }
        
        new_doc_id = firestore_wrapper.add_document(new_version)
        logger.info(f"Created new table version with version_id: {version_id} for table: {metadata['Name']}, view: {metadata['ViewName']}")
        
        batch_operations = [
            {'operation': 'update', 'doc_id': doc['id'], 'data': {'latest': False}}
            for doc in existing_versions if doc.get('latest', False)
        ]
        
        if batch_operations:
            firestore_wrapper.batch_write(batch_operations)
            logger.info(f"Updated 'latest' flag for {len(batch_operations)} existing versions")


class UpsertTableChecksumStrategy(BaseTableChecksumStrategy):
    def _perform_update(self, firestore_wrapper: FirestoreWrapper, data: List[Dict[str, Any]], config: PipelineConfig, data_processor: DataProcessor) -> None:
        view_table_data, metadata, table_checksum = self._prepare_table_data(config, data_processor)
        
        doc_id = metadata['Name']
        
        new_doc = {
            'update_type': config.update_type.value,
            'write_timestamp': firestore.SERVER_TIMESTAMP,
            'version_id': version_id,
            'table_checksum': table_checksum,
            'table_data': view_table_data,
            'metadata': metadata
        }
        
        existing_doc = firestore_wrapper.get_document(doc_id)
        
        if existing_doc:
            existing_fields = set(existing_doc.get('table_data', {}).keys())
            new_fields = set(view_table_data.keys())
            
            added_fields = new_fields - existing_fields
            removed_fields = existing_fields - new_fields
            
            if added_fields:
                logger.info(f"New fields added: {added_fields}")
            if removed_fields:
                logger.info(f"Fields removed: {removed_fields}")
            
            if existing_doc.get('table_checksum') == table_checksum:
                logger.info(f"No changes detected for table '{metadata['Name']}' with view '{metadata['ViewName']}'. Skipping update.")
                return
            else:
                logger.info(f"Changes detected for table '{metadata['Name']}' with view '{metadata['ViewName']}'. Updating document.")
        else:
            logger.info(f"No existing document found for table '{metadata['Name']}' with view '{metadata['ViewName']}'. Creating new document.")
        
        firestore_wrapper.set_document(doc_id, new_doc)
        logger.info(f"Document for table '{metadata['Name']}' with view '{metadata['ViewName']}' has been set with new data and checksum: {table_checksum}")

# =============================================================================================================================================================
# FACTORY
# =============================================================================================================================================================

class UpdateStrategyFactory:
    _strategies = {
        UpdateType.FULL_REFRESH: FullRefreshStrategy,
        UpdateType.REPLACE: ReplaceStrategy,
        UpdateType.VERSIONED: VersionedStrategy,
        UpdateType.SOFT_DELETE: SoftDeleteStrategy,
        UpdateType.UPSERT_CHECKSUM: UpsertChecksumStrategy,
        UpdateType.UPSERT_CHECKSUM_WITH_DELETE: UpsertChecksumWithDeleteStrategy,
        UpdateType.VERSIONED_SET: VersionedSetStrategy,
        UpdateType.VERSIONED_CHECKSUM: VersionedChecksumStrategy,
        UpdateType.VERSIONED_TABLE_CHECKSUM: VersionedTableChecksumStrategy,
        UpdateType.UPSERT_TABLE_CHECKSUM: UpsertTableChecksumStrategy 
    }

    @classmethod
    def get_strategy(cls, update_type: UpdateType) -> BaseUpdateStrategy:
        strategy_class = cls._strategies.get(update_type)
        if strategy_class is None:
            raise ValueError(f"Unsupported update type: {update_type}")
        return strategy_class()

    @classmethod
    def register_strategy(cls, update_type: UpdateType, strategy_class: type[BaseUpdateStrategy]) -> None:
        cls._strategies[update_type] = strategy_class
