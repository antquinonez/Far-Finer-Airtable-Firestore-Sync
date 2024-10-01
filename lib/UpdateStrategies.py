from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime
import time
import logging
from .AirtablePipelineConfigs import PipelineConfig, UpdateType
from .FirestoreWrapper import FirestoreWrapper
from .DataProcessor import DataProcessor
import pytz
from google.cloud import firestore

logger = logging.getLogger(__name__)

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
            
            existing_docs = firestore_wrapper.query_documents(config.primary_key, '==', primary_key_value)
            
            if existing_docs:
                existing_doc = existing_docs[0]
                new_checksum = data_processor.calculate_checksum(record, source_fields)
                existing_checksum = data_processor.calculate_checksum(existing_doc, source_fields)
                
                if new_checksum != existing_checksum:
                    # Update the existing document to set 'latest' to False
                    firestore_wrapper.update_document(existing_doc['id'], {'latest': False})
                    
                    # Create a new version of the document
                    new_record = {
                        **record,
                        'update_type': config.update_type.value,
                        'write_timestamp': firestore.SERVER_TIMESTAMP,
                        'version': version_id,
                        'latest': True
                    }
                    logger.debug(f"Adding new version of document, data types: {[(k, type(v)) for k, v in new_record.items()]}")
                    firestore_wrapper.add_document(new_record)
                    logger.info(f"Created new version of document with {config.primary_key}: {primary_key_value}")
                else:
                    logger.debug(f"No changes detected for document with {config.primary_key}: {primary_key_value}. Skipping update.")
            else:
                # This is a new record, create it with version 1
                new_record = {
                    **record,
                    'update_type': config.update_type.value,
                    'write_timestamp': firestore.SERVER_TIMESTAMP,
                    'version': version_id,
                    'latest': True
                }
                logger.debug(f"Adding new document, data types: {[(k, type(v)) for k, v in new_record.items()]}")
                firestore_wrapper.add_document(new_record)
                logger.info(f"Inserted new document with {config.primary_key}: {primary_key_value}")


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

class UpdateStrategyFactory:
    _strategies = {
        UpdateType.FULL_REFRESH: FullRefreshStrategy,
        UpdateType.REPLACE: ReplaceStrategy,
        UpdateType.VERSIONED: VersionedStrategy,
        UpdateType.SOFT_DELETE: SoftDeleteStrategy,
        UpdateType.UPSERT_CHECKSUM: UpsertChecksumStrategy,
        UpdateType.UPSERT_CHECKSUM_WITH_DELETE: UpsertChecksumWithDeleteStrategy,
        UpdateType.VERSIONED_SET: VersionedSetStrategy,
        UpdateType.VERSIONED_CHECKSUM: VersionedChecksumStrategy
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