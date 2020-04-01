# Data Modeling with Apache Cassandra

Fictional music streaming startup Sparkify wants to analyze their streaming data
on songs and user activity, particularly what songs users are listening to.
Their data is initially stored in CSV files on user activity.

I create the following to help them out:

- An Apache Cassandra database that optimizes Sparkify's querying of song play
data.
- An ETL pipeline to transfer data from the CSV files into Cassandra using
Python and CQL.

**NOTE:** This code runs in a Cassandra Docker container as outlined at the
bottom of this README to avoid the need to download Cassandra to your machine.
The connections specified in all python modules and Jupyter notebooks in this
repository are specified to connect to the container if it is configured
correctly. See instructions at the bottom of this README to run the container.

## Database

![](/images/sparkify_tables.jpeg?raw=true)

 The database is organized into three tables to optimize for three queries:
 1. `song_info`: Give the artist, song title and song's length in the music app
 history that was heard during  sessionId = 338, and itemInSession  = 4.

 PRIMARY KEY (session_id, item_in_session)
 2. `users_songs`: Give only the following: name of artist, song (sorted by
   itemInSession) and user (first and last name) for userid = 10, sessionid =
   182

 PRIMARY KEY (user_id, session_id, item_in_session)
 3. `user_name`: Give every user name (first and last) in my music app history
 who listened to the song 'All Hands Against His Own'

 Primary Key (song, user_id)

Of course, these tables can be used to satisfy any other queries that filter on
the corresponding partition key and clustering columns.

## Repo Organization
 The database can be created and filled by running scripts in the following
 order:
1. **create_tables.py:** This module connects to the Cassandra container,
drops existing tables, creates all three tables, and closes the connection to
the container.
2. **etl.py:** This module combines and filters song play CSV files into a
single file with data to be inserted into cassandra. It then connects to the
sparkify Cassandra keyspace (in the container) and inserts the data.

The last module, **cql_queries.py** contains all of the CQL queries used for
both read and write queries called from **create_tables.py** and **etl.py**.

**test.ipynb** can be used to write queries to test each of the tables.

## How to Use - Docker

Clone this repo to your desired directory and install
[docker](https://docs.docker.com/). Create an account if you haven't already.

Login to your account via the terminal (or Docker Desktop)

```
docker login docker.io
```
Pull the Cassandra docker image.
```
docker pull cassandra
```
Run the container
```
docker run --name cassandra-container -p 127.0.0.1:9042:9042 -d cassandra
```

Now you're able to run the scripts in the order specified above. All of the
connections specify the port 9042, so you will automatically connect to the
container's Cassandra database.

Once you're done and want to close out
```
docker stop cassandra-container
docker rm cassandra-container
```
## Acknowledgements
- The erd diagram was created using [Lucidchart](lucidchart.com)
