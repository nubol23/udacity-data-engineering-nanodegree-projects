import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list inside `sql_queries.py`.

    :param cur: cursor in the database
    :param conn: connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list inside `sql_queries.py`.

    :param cur: cursor in the database
    :param conn: connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Drop tables")
    drop_tables(cur, conn)
    print("Create tables")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()