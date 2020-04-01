"""
This module connects to the cassandra container, drops existing tables,
creates all three tables, and closes the connection to the container.

Author: M. Sanchez-Ayala 3/31/2020
"""
from cassandra.cluster import Cluster
from cql_queries import create_table_queries, drop_table_queries


def create_keyspace():
    """
    Returns
    -------
    cluster and session: cassandra Cluster object and an associated connection.

    A connection is established with the Cassandra container. A keyspace,
    sparkify, is created and set to the session.
    """

    # Create a connection to the cassandra docker container
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS
          sparkify
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
        }
    """)

    session.set_keyspace('sparkify')

    return cluster, session


def drop_tables(session):
    """
    Any existing tables in sparkify are dropped as specified by the queries in
    cql_queries.py.

    Parameters
    ----------
    session: a connection to the cassandra cluster.
    """
    for query in drop_table_queries:
        session.execute(query)


def create_tables(session):
    """
    All tables in sparkify are created as specified by the queries in
    cql_queries.py.

    Parameters
    ----------
    session: a connection to the cassandra cluster.
    """
    for query in create_table_queries:
        session.execute(query)


def main():
    """
    Bundles up the script: connects to cassandra, drops any existing tables,
    creates all three tables, and then closes the connection to the database.
    """
    # Create a connection to the cassandra docker container
    cluster, session = create_keyspace()

    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
