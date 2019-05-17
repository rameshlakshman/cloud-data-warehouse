CLOUD DATAWAREHOUSE : SPARKIFY
==============================
Purpose : 
This cloud datawarehouse is built for moving the sparkify processes and data into cloud.
Business needs to know the below key information generated from the new music streaming app for Sparkify.
    => Metadata for Songs being played
    => User-activity on the app.

SOURCE 
======
Source of data in these tables are JSON logs stored in S3 bucket.
Two feeds into the process are -
    => SONG_DATA formatted as JSON logs
    => LOG_DATA formatted as JSON logs

TARGET
======
The two JSON files ('song metadata' & 'log data of user activity') are read,
staged into stg_events & stg_songs respectively, and then loaded into below tables.
    DIMENSION TABLES - 
        'SONGS' 
        'USERS'
        'ARTISTS'
        'TIME' 
    FACT TABLE 
        'SONGPLAYS'

DATA MODEL
==========
    DIMENSION TABLES
    ================
        SONG_DATA feed contains information about the SONG and ARTISTS (i.e. song metadata). 
                Hence, these two information are loaded into separate DIMENSIONAL tables in SONGS & ARTISTS tables respectively.
            =>SONGS table holds below attributes :
                    song_id, song_title, artist, song_year, duration
            =>ARTISTS table holds below attributes :
                    artist_id, artist_name, location, latitude, longitude

        LOG_DATA feed contains information about user-activity in the app. 
                Hence, the information in this are loaded into two separate DIMESNTIONAL tables in TIME & USERS & SONGPLAYS tables respectively.
            => USERS table holds below attributes :
                    user_id, first_name, last_name, gender, level
            => TIME table holds below attributes :
                    start_time, hour, day, week, month, year, weekday

    FACT TABLE
    ==========
        SONGPLAYS table - This is a FACT table. Data is extracted from the LOG_DATA feed with lookup information 
                pulled from SONGS & ARTISTS dimensional tables.
            => This table holds below attributes available for analysis team for analysis.
                songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

ETL PROCESSES
=============
The process is accomplished in two major steps.
(1) Load JSON data from S3 bucket into Staging tables in Redshift
(2) Staging tables data is processed/transformed into target tables

    S3 to STAGING :
    ===============
    JSON files are read and staged into the redshift cluster
        Source - LOG_DATA  &  SONG_DATA
        Target - stg_events & stg_songs

    STAGING to DIM/FACT tables :
    ============================
    (1) stg_songs data is parsed and loaded into SONGS_STG, ARTISTS_STG
    (2) For matching keys between SONGS_STG & SONGS, delete rows from SONGS_STG
    (3) For matching keys between ARTISTS_STG & ARTISTS, delete rows from ARTISTS_STG
    (4) Perform inserts from SONGS_STG, ARTISTS_STG into SONGS, ARTISTS respectively
    (5) stg_events data is parsed & loaded into USERS_STG, TIME_STG, SONGPLAYS_STG
    (6) For matching keys between USERS_STG & USERS, update 'LEVEL' from _STG into USERS
    (7) Then delete from USERS_STG for matching keys with USERS.
    (8) For matching keys between TIME_STG & TIME, delete rows from TIME_STG
    (9) For matching keys between SONGPLAYS_STG & SONGPLAYS, delete rows from SONGPLAYS_STG
    (10)Perform inserts from USERS_STG, TIME_STG, SONGPLAYS_STG into respective targets.
    
              
FILES USED
==========
(1) sql_queries.py       => This holds drop/create/insert statements for REDSHIFT cloud tables
(2) create_tables.py     => This create aws_role, aws_cluster, drop/create staging & target tables that are used.
(3) etl.py               => This contains python code that loads S3:JSON to staging, and staging into destinationt ables.
(4) dwh.cfg              => Configuration file that has the parameters to be used in the code


HOW TO RUN
==========
(1) Run the 'create_tables.py' to
    => create role if missing
    => create cluster if missing
    => ensure security group accepts inbound tcp traffic
    => assign security group to cluster
    => drop and create staging and destination tables
    
(2) After (1) is done, execute 'etl.py' to 
    => Move JSON datasets into STAGING TABLES.
    => STAGING TABLES extract-transform-load to load redshift tables.

