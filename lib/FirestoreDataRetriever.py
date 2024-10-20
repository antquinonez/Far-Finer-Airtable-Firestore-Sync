from google.cloud import firestore
import logging

class FirestoreDataRetriever:
    def __init__(self, db_name='(default)'):
        self.db = firestore.Client(database=db_name)
        self.logger = logging.getLogger(__name__)

    def get_document_data(self, collection:str, doc_id:str):
        doc_ref = self.db.collection(collection).document(doc_id)
        doc = doc_ref.get()

        if doc.exists:
            doc_data = doc.to_dict()
            self.logger.info(f"{collection.capitalize()} Document ID: {doc_id}")
            self.logger.info(f"{collection.capitalize()} Document data: {doc_data}")
            return doc_data
        else:
            self.logger.info(f"No such {collection} document: {doc_id}")
            return None
