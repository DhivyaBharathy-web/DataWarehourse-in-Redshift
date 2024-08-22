import configparser
import psycopg2
from sql_queries import select_number_rows_queries


def get_tables_rows(cur, conn):
    """
    Gets the number of rows stored into each table
    """
    for query in select_number_rows_queries:
        print('\n'.join(('', 'Running:', query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)


def main():
    """
    Runs analytical queries
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to AWS Redshift')
    cur = conn.cursor()

    # Analytical queries
    get_tables_rows(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
