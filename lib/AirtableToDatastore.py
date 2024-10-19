import logging
from typing import List, Dict, Any
from time import time

from .AirtableDataFetcher import AirtableDataFetcher
from .DataProcessor import DataProcessor
from .FirestoreWrapper import FirestoreWrapper
from .UpdateStrategies import UpdateStrategyFactory
from .AirtablePipelineConfigs import PipelineConfig

logger = logging.getLogger(__name__)


class AirtableToDatastore:
    def __init__(self, config: PipelineConfig):
        config.validate()
        self.config = config

        self.firestore_wrapper = FirestoreWrapper(config.datastore)
        self.data_fetcher = AirtableDataFetcher(config.airtable)
        self.data_processor = DataProcessor(self.data_fetcher.fetch_field_types())
        self.update_strategy = UpdateStrategyFactory.get_strategy(config.update_type)

    def validate(self):
        if not all([self.base_id, self.table_name, self.api_key]):
            raise ValueError("All Airtable configurations must be set")     

    def run_pipeline(self) -> None:
        """
        Execute the entire data pipeline from Airtable to Firestore.
        """
        start_time = time()
        try:
            logger.info("Starting data pipeline")
            
            airtable_data = self.data_fetcher.fetch_data(view_name=self.config.airtable.view_name)
            logger.info(f"Fetched {len(airtable_data)} records from Airtable")
            
            processed_data = self.process_data(airtable_data)
            logger.info(f"Processed {len(processed_data)} records")
            
            self.update_strategy.update(self.firestore_wrapper, processed_data, self.config, self.data_processor)
            
            end_time = time()
            duration = end_time - start_time
            logger.info(f"Data pipeline completed successfully in {duration:.2f} seconds")
        except Exception as e:
            logger.error(f"Data pipeline failed: {e}")
            raise

    def process_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process the fetched Airtable data.
        """
        processed_data = self.data_processor.process_data(data)
        return self.data_processor.process_duplicate_names(processed_data, self.config.primary_key)