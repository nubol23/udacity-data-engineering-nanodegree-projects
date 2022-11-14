# Sparkify Data Warehousing

This project contains the scripts necessary to extract metadata and interactions between the users and the app into staging tables in Redshift, process it and load it into a relational structure for fast quering.

The data is reorganized into a Star Schema with a fact table of interactions and dimension tables containing the users, songs, artists and times information.

For the extraction we need to get the data from json files, convert them to tables without a structure wich we call staging tables, and then reformat the data into facts and dimension tables.

## How to run the scripts

1. Rename the file `dwh.cfg.example` to `dwh.cfg`.
2. Add missing credentials and connection data
3. Run the `create_tables.py` file
   ```bash
   python create_tables.py
   ```
4. Run the `etl.py` file
   ```bash
   python etl.py
   ```

## What does each file do?

* `create_tables.py`: drops all existing tables, and create new ones for the required structure.
* `etl.py`: loads the data from s3 and inserts it into the corresponding analytics oriented tables.
* `sql_queries.py`: raw queries in string format to be used to interact with the database in `etl.py`.