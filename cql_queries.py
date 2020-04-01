"""
This module contains all of the DROP, CREATE, INSERT, and SELECT queries
for the sparkify cassandra keyspace.

Author: M. Sanchez-Ayala 3/31/2020
"""
### CREATE TABLES ###

song_info_create = """
    CREATE TABLE IF NOT EXISTS
      song_info (
        artist text,
        item_in_session int,
        length float,
        session_id int,
        song text,
        PRIMARY KEY (session_id, item_in_session)
      )
"""

users_songs_create = """
    CREATE TABLE IF NOT EXISTS
      users_songs (
        artist text,
        first_name text,
        item_in_session int,
        last_name text,
        session_id int,
        song text,
        user_id int,
        PRIMARY KEY (user_id, session_id, item_in_session)
      )
"""

user_name_create = """
    CREATE TABLE IF NOT EXISTS
      user_name (
        last_name text,
        first_name text,
        song text,
        user_id int,
        PRIMARY KEY (song, user_id)
      )
"""

### INSERT DATA ###

song_info_insert = """
    INSERT INTO
      song_info (
        artist,
        item_in_session,
        length,
        session_id,
        song
      )
    VALUES
      (%s, %s, %s, %s, %s)
"""

users_songs_insert = """
    INSERT INTO
      users_songs (
        artist,
        first_name,
        item_in_session,
        last_name,
        session_id,
        song,
        user_id
      )
    VALUES
      (%s, %s, %s, %s, %s, %s, %s)
"""

user_name_insert = """
    INSERT INTO
      user_name (
        first_name,
        last_name,
        song,
        user_id
      )
    VALUES
      (%s, %s, %s, %s)
"""

### SELECT STATEMENTS ###

select_query_1 = """
    SELECT
      artist,
      song,
      length
    FROM
      song_info
    WHERE
      session_id = 338 AND
      item_in_session = 4
"""

select_query_2 = """
    SELECT
      artist,
      song,
      first_name,
      last_name
    FROM
      users_songs
    WHERE
      user_id = 10 AND
      session_id = 182
"""

select_query_3 = """
    SELECT
      first_name,
      last_name
    FROM
      user_name
    WHERE
      song = 'All Hands Against His Own'
"""

### DROP TABLES ###

song_info_drop = "DROP TABLE IF EXISTS song_info"

users_songs_drop = "DROP TABLE IF EXISTS users_songs"

user_name_drop = "DROP TABLE IF EXISTS user_name"

### QUERIES LISTS ###

create_table_queries = [song_info_create, users_songs_create, user_name_create]

drop_table_queries = [song_info_drop, users_songs_drop, user_name_drop]
