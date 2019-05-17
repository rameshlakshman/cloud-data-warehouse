import configparser
import boto3


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

iam = boto3.client('iam',aws_access_key_id=config.get('AWS','KEY'),
                    aws_secret_access_key=config.get('AWS','SECRET'),
                    region_name='us-west-2'
                  )

try:
    ROLE_ARN = iam.get_role(RoleName=config.get('IAM_ROLE','IAM_ROLE_NAME'))['Role']['Arn']
except Exception as e:
    ROLE_ARN = ''
    print(e)

S3_LOG_DATA   =config.get("S3","LOG_DATA")
S3_LOG_JSON   =config.get("S3","LOG_JSONPATH")
S3_SONG_DATA  =config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"
user_stg_table_drop       = "DROP TABLE IF EXISTS users_stg"
song_stg_table_drop       = "DROP TABLE IF EXISTS songs_stg"
artist_stg_table_drop     = "DROP TABLE IF EXISTS artists_stg"
time_stg_table_drop       = "DROP TABLE IF EXISTS time_stg"
songplay_stg_table_drop   = "DROP TABLE IF EXISTS songplays_stg"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stg_events 
                                        (artist_name   varchar,
                                         ev_auth       varchar,
                                         first_name    varchar,
                                         gender        char(1),
                                         iteminsession int,
                                         last_name     varchar,
                                         ev_length     numeric,
                                         level         varchar,
                                         location      varchar,
                                         method        varchar,
                                         page          varchar,
                                         registration  numeric,
                                         sessionid     int,
                                         song_title    varchar,
                                         status        int,
                                         ts            varchar,
                                         useragent     varchar,
                                         userid        int
                                        )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stg_songs 
                                        (song_id           varchar,
                                         num_songs         int,
                                         title             varchar,
                                         artist_name       varchar,
                                         artist_latitude   decimal,
                                         year              int,
                                         duration          decimal,
                                         artist_id         varchar,
                                         artist_longitude  decimal,
                                         artist_location   varchar
                                         )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                                        (songplay_id bigint IDENTITY(0,1) NOT NULL distkey,
                                         start_time timestamp NOT NULL,                             
                                         user_id int NOT NULL,                                 
                                         level varchar,                                        
                                         song_id varchar,                                      
                                         artist_id varchar,                                    
                                         session_id varchar NOT NULL,                          
                                         location varchar,                                     
                                         user_agent varchar,
                                         CONSTRAINT pk_tmusrses_id 
                                         UNIQUE (start_time, user_id, session_id)
                                        )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                                    (user_id int PRIMARY KEY, 
                                     first_name	varchar, 
                                     last_name varchar, 
                                     gender char(1), 
                                     level varchar
                                    )
                                    diststyle all
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                                    (song_id varchar PRIMARY KEY, 
                                     title varchar, 
                                     artist_id varchar distkey, 
                                     year int, 
                                     duration numeric 
                                    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                                      (artist_id varchar PRIMARY KEY distkey, 
                                       artist_name varchar, 
                                       location varchar, 
                                       latitude numeric, 
                                       longitude numeric
                                      )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                                    (start_time timestamp PRIMARY KEY sortkey distkey, 
                                     hour int, 
                                     day int,
                                     week int, 
                                     month int, 
                                     year int, 
                                     weekday int
                                    )
""")

# STAGING TABLES

staging_events_copy = ("""
                            copy stg_events 
                            from '{}'
                            iam_role '{}'
                            json '{}' ;
                           """).format(S3_LOG_DATA, ROLE_ARN, S3_LOG_JSON)

staging_songs_copy = ("""
                        copy stg_songs 
                        from '{}'
                        iam_role '{}'
                        json 'auto';
                      """).format(S3_SONG_DATA, ROLE_ARN)

# work TABLES
song_stg_table_create = ("""CREATE TABLE songs_stg AS 
                              select distinct song_id, 
                                 title, 
                                 artist_id, 
                                 year, 
                                 duration 
                              from stg_songs
""")

user_stg_table_create = ("""CREATE TABLE users_stg AS 
                              select distinct userid, 
                               first_name, 
                               last_name, 
                               gender, 
                               level 
                              from stg_events
                              where userid is not null
""")

artist_stg_table_create = ("""CREATE TABLE artists_stg AS
                               select distinct artist_id, 
                                  artist_name, 
                                  artist_location, 
                                  artist_latitude, 
                                  artist_longitude 
                                from stg_songs
""")

time_stg_table_create = ("""CREATE TABLE time_stg AS
                              SELECT START_TIME, 
                              EXTRACT (HOUR FROM START_TIME) as hour_, 
                              EXTRACT(DAY FROM START_TIME) as day_, 
                              EXTRACT(WEEK FROM START_TIME) as week_,
                              EXTRACT(MONTH FROM START_TIME) as month_, 
                              EXTRACT(YEAR FROM START_TIME) as year_, 
                              EXTRACT(WEEKDAY FROM START_TIME) as weekday_
                          FROM (select DISTINCT TIMESTAMP'epoch' + CAST(ts AS BIGINT)/1000 * INTERVAL '1 SECOND' AS START_TIME 
                                from stg_events)
""")

songplay_stg_table_create = ("""CREATE TABLE songplays_stg as
                                SELECT TIMESTAMP'epoch' + CAST(ts AS BIGINT)/1000 * INTERVAL '1 SECOND' as start_time,
                                      a.userid, a.level, b.song_id, 
                                      c.artist_id, a.sessionid, 
                                      a.location, a.useragent
                                  from stg_events a
                                  join songs b on a.song_title = b.title
                                  join artists c on b.artist_id = c.artist_id
""")

# FINAL TABLES
songplay_table_delete = ("""delete from songplays_stg  
                               using songplays
                               where songplays_stg.userid = songplays.user_id
                                 and songplays_stg.song_id = songplays.song_id
                                 and songplays_stg.start_time = songplays.start_time
""")                                 

songplay_table_insert = ("""INSERT INTO songplays 
                                 (start_time,
                                  user_id,
                                  level,
                                  song_id,
                                  artist_id,
                                  session_id,
                                  location,
                                  user_agent)
                            SELECT start_time,
                                  userid,
                                  level,
                                  song_id,
                                  artist_id,
                                  sessionid,
                                  location,
                                  useragent
                              from songplays_stg
""")

user_table_update = ("""update users  
                           set level = users_stg.level
                         from users_stg 
                         where users.user_id = users_stg.userid
""")

user_table_delete = ("""delete from users 
                         using users_stg 
                         where users.user_id = users_stg.userid
""")

user_table_insert = ("""INSERT INTO users 
                              (user_id,
                               first_name,
                               last_name,
                               gender,
                               level)
                        select userid, 
                               first_name, 
                               last_name, 
                               gender, 
                               level 
                          from users_stg
""")

song_table_delete = ("""delete from songs_stg 
                         using songs 
                         where songs_stg.song_id = songs.song_id
""")

song_table_insert = ("""INSERT INTO songs 
                             (song_id,
                              title,
                              artist_id,
                              year,
                              duration)
                        select distinct song_id, 
                               title, 
                               artist_id, 
                               year, 
                               duration 
                          from songs_stg
""")

artist_table_delete = ("""delete from artists_stg
                           using artists 
                           where artists_stg.artist_id = artists.artist_id
""")


artist_table_insert = ("""INSERT INTO artists 
                             (artist_id,
                              artist_name,
                              location,
                              latitude,
                              longitude)
                          select distinct artist_id, 
                              artist_name, 
                              artist_location, 
                              artist_latitude, 
                              artist_longitude 
                            from artists_stg                           
""")

time_table_delete = ("""delete from time_stg
                           using time 
                           where time_stg.start_time = time.start_time
""")

time_table_insert = ("""INSERT INTO time 
                             (start_time,
                              hour,
                              day,
                              week,
                              month,
                              year,
                              weekday)
                        SELECT start_time,
                              hour_,
                              day_,
                              week_,
                              month_,
                              year_,
                              weekday_
                          FROM time_stg 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert, user_table_insert, time_table_insert, songplay_table_insert]
song_table      = [song_stg_table_create, song_table_delete, song_table_insert, song_stg_table_drop]
artist_table    = [artist_stg_table_create, artist_table_delete, artist_table_insert, artist_stg_table_drop]
user_table      = [user_stg_table_create, user_table_update, user_table_delete, user_table_insert, user_stg_table_drop]
time_table      = [time_stg_table_create, time_table_delete, time_table_insert, time_stg_table_drop]
songplay_table  = [songplay_stg_table_create, songplay_table_delete, songplay_table_insert, songplay_stg_table_drop]
process_table   = [song_table, artist_table, user_table, time_table, songplay_table]