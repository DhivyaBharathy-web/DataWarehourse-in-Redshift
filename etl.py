import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes queries aimed to load information to Redshift Cluster from a S3 bucket
    """
    for query in copy_table_queries:
        print('\n'.join(('\nLoading STAGING DATA:', query)))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes queries aimed to insert data into the Star Schema
    """
    for query in insert_table_queries:
        print('\n'.join(('\nInserting into STAR SCHEMA:', query)))
        cur.execute(query)
        conn.commit()


def main():
    """
    First creates the staging tables with information from the specified S3 bucket and then inserts them into the fact
    and dimension tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to AWS Redshift')
    cur = conn.cursor()

    # ETL queries
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
