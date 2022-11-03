"""
This file contains all the SQL statements used in this project. This includes
DROP TABLE, CREATE TABLE, COPY, and INSERT INTO statements. A config file is
used to read configuration parameters used in the process.
"""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = (
    """
    DROP
        TABLE IF EXISTS staging_events;
    """
)
staging_songs_table_drop = (
    """
    DROP
        TABLE IF EXISTS staging_songs;
    """
)
songplay_table_drop = (
    """
    DROP
        TABLE IF EXISTS songplays;
    """
)
user_table_drop = (
    """
    DROP
        TABLE IF EXISTS users;
    """
)
song_table_drop = (
    """
    DROP
        TABLE IF EXISTS songs;
    """
)
artist_table_drop = (
    """
    DROP
        TABLE IF EXISTS artists;
    """
)
time_table_drop = (
    """
    DROP
        TABLE IF EXISTS time;
    """
)

# CREATE TABLES

staging_events_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length double precision,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration double precision,
        sessionId int,
        song varchar,
        status int,
        ts timestamp NOT NULL,
        userAgent varchar,
        userId int
    )
    """
)
staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        artist_id varchar NOT NULL,
        artist_latitude double precision,
        artist_location varchar,
        artist_longitude double precision,
        artist_name varchar,
        duration double precision,
        num_songs int,
        song_id varchar NOT NULL,
        title varchar,
        year smallint
    )
    """
)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id int IDENTITY(1, 1),
        start_time timestamp NOT NULL,
        user_id int,
        level varchar,
        song_id varchar NOT NULL,
        artist_id varchar NOT NULL,
        session__id int,
        location varchar,
        user_agent varchar,
        CONSTRAINT songplay_pk PRIMARY KEY (songplay_id)
    )
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users
    (
        user_id int,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar,
        CONSTRAINT users_pk PRIMARY KEY (user_id)
    )
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id varchar NOT NULL,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year smallint,
        duration double precision NOT NULL,
        CONSTRAINT songs_pk PRIMARY KEY (song_id)
    )
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id varchar NOT NULL,
        name varchar NOT NULL,
        location varchar,
        latitude double precision,
        longitude double precision,
        CONSTRAINT artists_pk PRIMARY KEY (artist_id)
    )
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time
    (
        start_time timestamp NOT NULL,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int,
        CONSTRAINT time_pk PRIMARY KEY(start_time)
    )
    """
)

# STAGING TABLES


staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    json {}
    TIMEFORMAT as 'epochmillisecs'
    region 'us-west-2';
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    json 'auto'
    region 'us-west-2';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays
    (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session__id,
        location,
        user_agent
    )
    SELECT se.ts, se.userId, se.level, ss.song_id, ss.artist_id, \
        se.sessionId, ss.artist_location, se.userAgent
    FROM staging_events se JOIN staging_songs ss
    ON se.artist = ss.artist_name
    WHERE se.page='NextSong'
    """
)

user_table_insert = (
    """
    INSERT INTO users
    (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
        SELECT DISTINCT (userId), firstName, lastName, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """
)

song_table_insert = (
    """
    INSERT INTO songs
    (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT(song_id), title, artist_id, year, duration
    FROM staging_songs
    """
)

artist_table_insert = (
    """
    INSERT INTO artists
    (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT(artist_id), artist_name, artist_location, \
        artist_latitude, artist_longitude
    FROM staging_songs
    """
)

time_table_insert = (
    """
    INSERT INTO time
    (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT ts, EXTRACT(hour from ts), EXTRACT(d from ts), \
        EXTRACT(w from ts), EXTRACT(mon from ts), EXTRACT(y from ts), \
        EXTRACT(dw from ts)
    FROM staging_events
    WHERE page='NextSong'
    """
)
# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]
