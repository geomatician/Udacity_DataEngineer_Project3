# Project 3: Data Warehouse

# Purpose of database in context of Sparkify and their goals

Sparkify wants to analyze data that they are collecting on songs and user activity in their music streaming application. They want to gain insights into the kinds of songs their users are currently listening to.

Currently, all of their data resides within JSON files of two types- song data files and log data files. Song data files contain information about songs and artists. Log data files contain information about application users and times. The analytics team at Sparkify wants to find a better way to organize and query this data to generate analytics. This can be done by setting up an ETL pipeline that 1) reads data from the JSON files, 2) transforms it into a useful structure (i.e. staging tables) for insertion into a database in Redshift, and 3) loads it into a collection of fact and dimension tables, after which optimized queries can be run on the database. With an enhanced organization of data, the analytics team can run customized, ad-hoc queries and generate useful analytics from the data. 

# How to run the Python scripts

You should already have a Redshift cluster created that contains a database named 'sparkify'. An IAM role
should exist that provides read access to S3 for the Redshift cluster. Once a Redshift cluster has been
created, provide the cluster connection and IAM role ARN information in the config file named dwh.cfg.

Clone the Git repository to a directory on your local machine. 

Then, navigate (```cd```) into this directory and run the commands in the following order:

```
python create_tables.py
python etl.py
```

Python 3.9.13 was used for this project. Prior to running the above scripts, ensure that the psycopg2 package is installed within the current Python environment. 

# Files in repository

1) create_tables.py - this connects to the sparkify database, drops all tables if they exist, and (re) creates these tables in the database. After running this script, the 'sparkify' database should have 7 empty tables- artists, songplays, songs, time, users, staging_events, and staging_songs. 
2) etl.py - this will load the data from the song and log JSON files into the staging tables, and from there into the fact and dimension tables that are created by create_tables.py.
3) sql_queries.py - this contains SQL queries to create tables and insert data into these tables for each of the seven tables used in this project. These queries are used in create_tables.py and etl.py. 
4) dwh.cfg - this is a configuration file used to store and retrieve configuration parameters such as the
Redshift cluster, S3 bucket locations, and IAM roles used to access S3 from Redshift.
5) log_json_path.json - This is a JSONPaths file that is used by the COPY command for the staging_events table
to load the data into the table from the log JSON files in S3. 
6) sample_log_event.json - This file contains a sample log event object.
7) sample_song.json - This file contains a sample set of song objects.

# Database schema design and ETL pipeline

The data gets initially loaded from the source S3 buckets into two staging tables named staging_events and staging_songs, respectively. The staging_events table is used to store log data and the staging_songs table is used to store song data. The staging tables are temporary tables that will be used to make changes to the target tables, such as updates or inserts. Then, from these staging tables, data is loaded into the fact table songplays and dimension tables songs, artists, users, and time. Each table has its own primary key that is unique and not null by definition. Some of the tables also have foreign keys that allow them to be associated with other tables in the database: for example, the songplays table has user_id, artist_id, and song_id fields, and the songs table has an artist_id field. With the combination of their primary and foreign keys through joins, customized SQL queries can be written to generate analytics across these tables. 

The data types of the fields within each table have been set properly. Furthermore, NOT NULL, PRIMARY KEY, and DISTINCT constraints have been introduced into sql_queries.py to prevent duplicates and non-null values from being populated into the tables. The ETL pipeline contained within etl.py, when run, extracts data from the JSON files, loads it into the staging tables, and then from there loads the data into the fact and dimension tables.

The songplays table is the core table (i.e. fact table) that contains foreign keys to the songs, artists, users, and time tables (i.e. dimension tables).  Together, they form a star schema with the songplays table at the center, and the other four tables around it. A star schema in the way organized in this project provides several benefits: 1) it makes queries easier with simple joins 2) we can perform aggregations on our data, and 3) we can relate the fact table with the dimension tables to perform cross-table analysis ; the fact table contains foreign keys which by definition are primary keys of the dimension tables. The dimension tables contain additional information about each type of entity- i.e. song, artist, user, and time, whereas the fact table displays the relationships between these different entities in one table using foreign keys. 

This serves as a justification for the database schema design and ETL pipeline. 

