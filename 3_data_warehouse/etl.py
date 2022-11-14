import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the json files into staging tables in Redshift for further processing
    running the queries in `copy_table_queries` list inside `sql_queries.py`.

    :param cur: cursor in the database
    :param conn: connection to the database
    :return:
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert the corresponding data to each fact and dimension table from the staging tables
    running the queries from `insert_table_queries` list inside `sql_queries.py`.

    :param cur: cursor in the database
    :param conn: connection to the database
    :return:
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # print("Loading")
    # load_staging_tables(cur, conn)
    print("Inserting")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()