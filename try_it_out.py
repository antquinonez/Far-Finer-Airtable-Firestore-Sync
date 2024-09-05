import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '.')))
from lib.AirtablePipelineConfigs import PipelineConfig, AirtableConfig, DatastoreConfig, UpdateType
from lib.AirtableToDatastore import AirtableToDatastore
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Setup variables
api_key = os.environ.get('AIRTABLE_API_KEY')
base_id = os.environ.get('AIRTABLE_BASE_ID')

table_name = 'airtable_table_name'

datastore_kind = 'some_datastore_collection'

project_id = 'your_project_id'
database_id = 'your database id'

# The primary key Airtable will use to identify records in the collection
primary_key = 'Name'

# Create configuration
airtable_config = AirtableConfig(base_id=base_id, table_name=table_name, api_key=api_key)
datastore_config = DatastoreConfig(project_id=project_id, database_id=database_id, kind=datastore_kind)

pipeline_config = PipelineConfig(
    airtable=airtable_config,
    datastore=datastore_config,
    primary_key="Name",
    update_type=UpdateType.UPSERT_CHECKSUM
)

# Create and run the pipeline
pipeline = AirtableToDatastore(pipeline_config)
pipeline.run_pipeline()


