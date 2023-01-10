import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id bigint identity(0,1) NOT NULL UNIQUE PRIMARY KEY,
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer,
        lastName varchar,
        length varchar,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId integer SORTKEY DISTKEY,
        song varchar,
        status varchar,
        ts bigint,
        userAgent varchar,
        userId integer 
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs integer,
        artist_id varchar SORTKEY DISTKEY,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar (500),
        artist_name varchar (500),
        song_id varchar,
        title varchar (500),
        duration decimal(10),
        year integer
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id integer identity(0,1) NOT NULL SORTKEY PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id int NOT NULL DISTKEY,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar(255)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer NOT NULL SORTKEY PRIMARY KEY,
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar NOT NULL SORTKEY PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar,
        year integer,
        duration decimal(10)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar NOT NULL SORTKEY PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude decimal(10),
        longitude decimal(10)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL SORTKEY PRIMARY KEY,
        hour smallint,
        day smallint,
        week smallint,
        month smallint,
        year smallint,
        weekday smallint
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT 
        DISTINCT timestamp with time zone 'epoch' + se.ts/1000 * interval '1 second', 
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events AS se INNER JOIN staging_songs AS ss
    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (
       user_id,
       first_name,
       last_name,
       gender,
       level
    )
    WITH uniq_staging_events AS (
        SELECT 
            userId,
            firstName,
            lastName,
            gender,
            level,
            ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
        FROM staging_events
        WHERE page = 'NextSong' AND userId IS NOT NULL
    )
    SELECT 
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM uniq_staging_events
    WHERE rank = 1;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT 
        DISTINCT song_id,
        title,
        artist_id,
        year, 
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT 
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT start_time, 
        extract(hour FROM start_time),
        extract(day FROM start_time),
        extract(week FROM start_time),
        extract(month FROM start_time),
        extract(year FROM start_time),
        extract(weekday FROM start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
