import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from the source S3 buckets containing the song and log JSON
    files into the staging tables staging_events and staging_songs.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Loads data from the staging tables staging_events and staging_songs into
    the fact and dimension tables songplays, users, songs, artists, and time
    tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads values from a config file and connects to a Redshift cluster
    that contains a database named sparkify. The config file stores
    connection information for the Redshift cluster, the ARN of an IAM role
    that allows the cluster to access S3, and the locations where the song
    and log JSON files are placed on S3. Finally, data is loaded into staging
    tables and the fact and dimension tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} \
                             port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
