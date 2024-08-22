import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes queries aimed to drop tables based on 'drop_table_queries' defined in sql_queries.py
    """
    for query in drop_table_queries:
        print('\n'.join(('\nExecuting DROP:', query)))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes queries aimed to create tables based on 'create_table_queries' defined in sql_queries.py
    """
    for query in create_table_queries:
        print('\n'.join(('\nExecuting CREATE:', query)))
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to redshift, drop existing tables (if any) and creates new tables based on sql_queries.py
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to AWS Redshift')
    cur = conn.cursor()

    # Creation queries
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
