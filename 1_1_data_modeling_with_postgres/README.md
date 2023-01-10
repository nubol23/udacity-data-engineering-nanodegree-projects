# Sparkify Data Modeling

This project contains the scripts necessary to ingest metadata and interactions between the users and the app.

As the principal goal is to be able to get insights from the data, we need to reorganize it into a star schema, in order to simplify calculation querys doing them directly in the fact tables.

For this we need to pre process the information, extracting the multiple records in json format, converting them to tabular data, performing the correcta data conversions and then inserting them to the database designed following the star schema, being our fact table, the one containing song play's interactions and our dimension tables, the user, artist abd song informations, together with the more granular time fields.

## How to run the scripts

First we must create a postgres user  with username `student` and password `student`. Also a dummy db called `studentdb` is necessary for the scripts to run.

1. Run the `create_tables.py` file
   ```bash
   python create_tables.py
   ```
2. Run the `etl.py` file
   ```bash
   python etl.py
   ```

## What does each file do?

* `create_tables.py`: drops all existing data and creates a new fresh database with new tables.
* `etl.ipyn`: notebook containing the step by step instructions and solutions for the etl pre processing.
* `etl.py`: contains all the functions and driver code to pre process the data and insert it into the database.
* `sql_queries.py`: raw queries in string format to be used to interact with the database in `etl.py`.
* `test.ipynb`: notebook containing sanity checks.