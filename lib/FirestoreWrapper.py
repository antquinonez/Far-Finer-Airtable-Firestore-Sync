from google.cloud import firestore
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import pytz
from .AirtablePipelineConfigs import DatastoreConfig
logger = logging.getLogger(__name__)


from google.cloud import firestore_v1

class FirestoreWrapper:
    def __init__(self, config: DatastoreConfig):
        self.config = config
        self.client = self._get_firestore_client()
        self.SERVER_TIMESTAMP = firestore_v1.SERVER_TIMESTAMP

    def _get_firestore_client(self) -> firestore.Client:
        try:
            return firestore.Client(
                project=self.config.project_id,
                database=self.config.database_id
            )
        except Exception as e:
            logger.error(f"Failed to create Firestore client: {e}")
            raise RuntimeError(f"Failed to create Firestore client: {e}")

    def clear_collection(self) -> None:
        docs = self.client.collection(self.config.kind).get()
        for doc in docs:
            doc.reference.delete()
        logger.info(f"Cleared all documents from collection {self.config.kind}")

    def add_document(self, data: Dict[str, Any]) -> str:
        try:
            if 'write_timestamp' in data:
                data['write_timestamp'] = self.SERVER_TIMESTAMP
            doc_ref = self.client.collection(self.config.kind).add(data)
            logger.debug(f"Added new document with ID: {doc_ref[1].id}")
            return doc_ref[1].id
        except Exception as e:
            logger.error(f"Error adding document: {e}")
            raise RuntimeError(f"Error adding document: {e}")

    def update_document(self, doc_id: str, data: Dict[str, Any]) -> None:
        try:
            if 'write_timestamp' in data:
                data['write_timestamp'] = self.SERVER_TIMESTAMP
            self.client.collection(self.config.kind).document(doc_id).set(data, merge=True)
            logger.debug(f"Updated document with ID: {doc_id}")
        except Exception as e:
            logger.error(f"Error updating document {doc_id}: {e}")
            raise RuntimeError(f"Error updating document {doc_id}: {e}")

    def delete_document(self, doc_id: str) -> None:
        try:
            self.client.collection(self.config.kind).document(doc_id).delete()
            logger.debug(f"Deleted document with ID: {doc_id}")
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
            raise RuntimeError(f"Error deleting document {doc_id}: {e}")

    def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        try:
            doc = self.client.collection(self.config.kind).document(doc_id).get()
            return {'id': doc.id, **doc.to_dict()} if doc.exists else None
        except Exception as e:
            logger.error(f"Error retrieving document {doc_id}: {e}")
            raise RuntimeError(f"Error retrieving document {doc_id}: {e}")

    def query_documents(self, field: str, operator: str, value: Any) -> List[Dict[str, Any]]:
        try:
            docs = self.client.collection(self.config.kind).where(field, operator, value).get()
            return [{'id': doc.id, **doc.to_dict()} for doc in docs]
        except Exception as e:
            logger.error(f"Error querying documents: {e}")
            raise RuntimeError(f"Error querying documents: {e}")

    def query_documents_not_equal(self, field: str, value: Any) -> List[Dict[str, Any]]:
        try:
            docs = self.client.collection(self.config.kind).where(field, '!=', value).get()
            return [{'id': doc.id, **doc.to_dict()} for doc in docs]
        except Exception as e:
            logger.error(f"Error querying documents: {e}")
            raise RuntimeError(f"Error querying documents: {e}")

    def batch_write(self, operations: List[Dict[str, Any]]) -> None:
        try:
            batch = self.client.batch()
            for op in operations:
                if op['operation'] == 'create':
                    doc_ref = self.client.collection(self.config.kind).document()
                    if 'write_timestamp' in op['data']:
                        op['data']['write_timestamp'] = self.SERVER_TIMESTAMP
                    batch.set(doc_ref, op['data'])
                elif op['operation'] == 'update':
                    doc_ref = self.client.collection(self.config.kind).document(op['doc_id'])
                    if 'write_timestamp' in op['data']:
                        op['data']['write_timestamp'] = self.SERVER_TIMESTAMP
                    batch.update(doc_ref, op['data'])
                elif op['operation'] == 'delete':
                    doc_ref = self.client.collection(self.config.kind).document(op['doc_id'])
                    batch.delete(doc_ref)
            batch.commit()
            logger.info(f"Batch write operation completed successfully")
        except Exception as e:
            logger.error(f"Error performing batch write: {e}")
            raise RuntimeError(f"Error performing batch write: {e}")