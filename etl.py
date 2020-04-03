"""
This module combines and filters song play CSV files into a single file with
data to be inserted into cassandra. It then connects to the sparkify Cassandra
keyspace and inserts the data.

NOTE:
create_tables.py must be run before this in order to make sure the tables
within sparkify are both created and empty.

test.ipynb can be used to test the etl pipeline.

Author: M. Sanchez-Ayala 3/31/2020
"""
import pandas as pd
from cassandra.cluster import Cluster
import os
import glob
import csv
from cql_queries import *

def extract_csvs():
    """
    Returns
    -------
    A single list containing all rows of the individual CSV files in the
    event_data directory.
    """
    # Get absolute filepath of event_data directory
    filepath = os.getcwd() + '/event_data'

    # join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(filepath,'*'))

    # Create an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # For every filepath in the file path list
    for f in file_path_list:

        # Read csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:

            # Create a csv reader object
            csvreader = csv.reader(csvfile)

            # Skip header row (column names)
            next(csvreader)

            # Extract each row of data and append
            for line in csvreader:
                full_data_rows_list.append(line)

    return full_data_rows_list

def transform_csvs(full_data_rows_list):
    """
    Writes
    -------
    A smaller CSV file in the current directory called
    event_datafile_full.csv that excludes non-song-playing entries in the user log.

    Parameters
    ----------
    full_data_rows_list: list of all rows of the individual CSV files in the
    event_data directory. It's the output of extract_csvs().

    """
    # Create custom dialect to parse CSV
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    # Create new csv to store data and instantiate a writer object
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')

        # Name columns
        writer.writerow([
            'artist',
            'first_name',
            'gender',
            'item_in_session',
            'last_name',
            'length',
            'level',
            'location',
            'session_id',
            'song',
            'user_id'
        ])

        # Iterate through each row in the data list
        for row in full_data_rows_list:

            # Filter out rows from CSVs that don't deal with song plays
            if row[0] != '':

                # Store only relevant features
                writer.writerow((
                    row[0], row[2], row[3], row[4],
                    row[5], row[6], row[7], row[8],
                    row[12], row[13], row[16]
                ))

def process_csvs():
    """
    Combines and filters all of the CSVs in the event_data directory into
    a new file event_datafile_new.csv that will be used to populate the
    Cassandra tables.

    Also prints out how many rows existed in total vs. in the final CSV.
    """
    # Extract raw data from CSV files
    full_data_rows_list = extract_csvs()

    print('Full combined CSV data contains', len(full_data_rows_list), 'rows.')

    # Transform into single, filtered CSV
    transform_csvs(full_data_rows_list)

    # check the number of rows in your csv file
    with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
        print('Filtered CSV File contains', sum(1 for line in f), 'rows.')


def load_data(session):
    """
    Opens the transformed CSV and loads each row into all the three tables
    as specified by the queries in cql_queries.py.

    Parameters
    ----------
    session: a connection to the cassandra cluster.
    """
    # Read filtered CSV
    with open('event_datafile_new.csv', encoding = 'utf8') as f:
        csvreader = csv.reader(f)

        # Skip header row (column names)
        next(csvreader)

        for line in csvreader:

            # First table
            session.execute(
                song_info_insert,
                (int(line[8]), int(line[3]), line[0], float(line[5]), line[9])
            )

            # Second table
            session.execute(
                users_songs_insert,
                (int(line[10]), int(line[8]), int(line[3]), line[0],
                line[1], line[4], line[9])
            )

            # Third table
            session.execute(
                user_name_insert,
                (line[9], int(line[10]), line[1], line[4])
            )


def main():
    """
    Bundles all ETL. Connects to sparkifydb, performs ETL on song data and then
    log data. Closes the connection to sparkifydb.
    """
    # Create a connection to the cassandra docker container, set the keyspace
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    session.set_keyspace('sparkify')

    # Extract and transform raw data
    process_csvs()

    # Load into Cassandra tables
    load_data(session)

    # End the session and connection
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
