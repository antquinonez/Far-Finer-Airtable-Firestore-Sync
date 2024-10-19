# %%
import os
import sys
from dotenv import load_dotenv
import logging

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '.')))
from lib.AirtablePipelineConfigs import PipelineConfig, AirtableConfig, DatastoreConfig, UpdateType
from lib.AirtableToDatastore import AirtableToDatastore
from lib.AirtableToDatastoreBuilder import AirtableToDatastoreBuilder
from lib.Secrets import Secrets
from google.cloud import firestore

# %%
_= Secrets.get_api_key('AIRTABLE_API_KEY')

# %%
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# %%
api_key  = os.getenv('AIRTABLE_API_KEY')
base_id = os.getenv('AIRTABLE_BASE_ID')

project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
database_id = os.getenv('FS_DATABASE_ID')
datastore_collection = os.getenv('FS_CONFIGS')

# %% [markdown]
# ## process hiring strategies

# %%
source_table = os.getenv('AT_STRATEGY_SPEC_TABLE_NAME')
source_view = os.getenv('AT_STRATEGY_SPEC_VIEW_NAME')
primary_key = os.getenv('AT_STRATEGY_SPEC_PK')

update_type = UpdateType.UPSERT_TABLE_CHECKSUM

# Create configuration
airtable_config = AirtableConfig(base_id=base_id, table_name=source_table, view_name=source_view, api_key=api_key)
datastore_config = DatastoreConfig(project_id=project_id, database_id=database_id, kind=datastore_collection)

pipeline_config = PipelineConfig(
    airtable=airtable_config,
    datastore=datastore_config,
    primary_key=primary_key,
    update_type=update_type,
)

# Create and run the pipeline
pipeline = AirtableToDatastore(pipeline_config)
pipeline.run_pipeline()

# %% [markdown]
# ## process candidate evaluation rules -- with builder for more extensive checks/setup

# %%
source_table = os.getenv('AT_CANDIDATE_EVAL_TABLE_NAME')
source_view = os.getenv('AT_CANDIDATE_EVAL_VIEW_NAME')
primary_key = os.getenv('AT_CANDIDATE_EVAL_PK')

update_type = UpdateType.UPSERT_TABLE_CHECKSUM

pipeline_config = (AirtableToDatastoreBuilder()
    .with_airtable_config(base_id=base_id, table_name=source_table, view_name=source_view, api_key=api_key)
    .with_datastore_config(project_id=project_id, database_id=database_id, kind=datastore_collection)
    .with_primary_key(primary_key)
    .with_update_type(update_type)
    .build())

# Create and run the pipeline
pipeline = AirtableToDatastore(pipeline_config)
pipeline.run_pipeline()

# %% [markdown]
# 

# %%



