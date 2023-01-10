# Sparkify Data Lake

This project contains the scripts necessary to load json files from a s3 bucket into spark dataframes
process them into dimensional tables and store in another s3 bucket

The data is reorganized into a Star Schema with a fact table of interactions and dimension tables containing the users, songs, artists and time information.

## How to run the scripts

1. Rename the file `dwh.cfg.example` to `dwh.cfg`.

2. Add missing credentials and connection data

3. Run the `etl.py` file

   ```bash
   python etl.py
   ```

## What does each file do?

* `etl.py`: loads the data from s3 process it, create parquet partition files and store them in a s3 bucket
* `experiments.ipynb`: notebook with the code for developing and testing the etl locally before writing it to `etl.py`