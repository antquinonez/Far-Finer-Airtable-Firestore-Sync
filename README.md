# Airtable-Firestore-Sync
A Python-based ETL pipeline for synchronizing data between Airtable and Google Cloud Firestore. This project offers flexible update strategies to easily manage data flow between these two platforms.

## Status

🚧 **Alpha Software** 🚧

This project is currently in alpha stage. While functional, it may contain bugs and is subject to significant changes. Use with caution.

Known issues:
- Needs tests
- Airtable Date-time fields are currently represented as strings
- The pipeline works with Airtable tables, but does not yet support Airtable views

I'm actively working on resolving these issues and adding new features. Contributions and feedback are welcome!


## Features

- Flexible configuration for Airtable and Firestore connections
- Multiple update strategies
- Efficient data processing and type conversion
- Comprehensive logging and error handling

## Prerequisites

- Python
- Google Cloud SDK (gcloud, etc)
- Airtable API key -- API works with a free account
- Google Cloud project with Firestore enabled; permissions setup, etc

## Installation

1. Clone this repository

2. Install the required dependencies:
pip install -r requirements.txt

3. Set up your Google Cloud credentials:
gcloud auth application-default login

# Usage
1.Setup the environment variables AIRTABLE_API_KEY and AIRTABLE_BASE_ID
2.Edit the "Setup variables" section in the try_it_out.py script

Run: python try_it_out.py

# Update Types
The pipeline supports several update strategies:

- FULL_REFRESH: Deletes all existing data in the Firestore collection and replaces it with the current Airtable data.
- REPLACE: For each record in Airtable, deletes any existing records with the same primary key in Firestore and inserts the new record.
- UPSERT_CHECKSUM: Compares each Airtable record with existing Firestore records using a checksum. Updates Firestore if there are changes, or inserts if the record is new.
- UPSERT_CHECKSUM_WITH_DELETE: Similar to UPSERT_CHECKSUM, but also deletes Firestore records that no longer exist in Airtable.
- VERSIONED: Creates a new version of each record in Firestore, marking the previous version as not latest.
- VERSIONED_SET: Creates a new set of all records if any changes are detected, using a version ID. Marks all previous records as not latest.

# Error Handling and Logging
The pipeline includes comprehensive error handling and logging. Check the logs for detailed information about the synchronization process and any issues that may occur.

# Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

# License
This project is licensed under the MIT License - see the LICENSE file for details.
