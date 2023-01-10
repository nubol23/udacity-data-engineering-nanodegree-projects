import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, 
    col,
    year, 
    month, 
    dayofmonth, 
    hour, 
    weekofyear, 
    date_format, 
    dayofweek,
    monotonically_increasing_id,
)
from pyspark.sql.types import FloatType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["KEYS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["KEYS"]['AWS_SECRET_ACCESS_KEY']

INPUT_S3_BUCKET = config['S3']['INPUT_S3_BUCKET']
OUTPUT_S3_BUCKET = config['S3']['OUTPUT_S3_BUCKET']


def create_spark_session():
    """Load extra packages and create a session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads song_data json files and use the information to
    create song and artist table.
    
    :param spark: spark session
    :param input_data: base path to load data (can be s3 or local path)
    :param output_data: base path to write data (can be s3 or local path)
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table\
    .write\
    .partitionBy("year", "artist_id")\
    .mode("overwrite")\
    .parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude")
    ).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Loads log json files to create user, time and songplay tables
    
    :param spark: spark session
    :param input_data: base path to load data (can be s3 or local path)
    :param output_data: base path to write data (can be s3 or local path)
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    user_table = df.select("userId", "firstName", "lastName", "gender", "level").distinct()
    
    # write users table to parquet files
    user_table.write.mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, FloatType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    
    # extract columns to create time table
    temp_df = (
        df
        .withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("month", month("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", dayofweek("start_time"))
    )
    time_table = temp_df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/*/*/*")

    # extract columns from joined song and log datasets to create songplays table 
    artists_table = spark.read.parquet(output_data + "artists")

    song_log_df = df.join(song_df, df.song == song_df.title)
    song_log_artist_df = song_log_df.join(artists_table, song_log_df.artist == artists_table.name)
    song_log_artist_time_df = song_log_artist_df.join(
        time_table,
        song_log_artist_df.start_time == time_table.start_time, 'left'
    )
    
    songplays_table = song_log_artist_time_df.select(
        monotonically_increasing_id().alias("songplay_id"),
        song_log_artist_df.start_time,
        col("userId").alias("user_id"),
        "level",
        "song_id",
        "artist_id", 
        "sessionId",
        song_log_df.location,
        col("userAgent").alias("user_agent"),
        "year",
        "month"
    ).repartition("year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table\
    .write.mode("overwrite")\
    .partitionBy("year", "month")\
    .parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = INPUT_S3_BUCKET
    output_data = OUTPUT_S3_BUCKET
    
    print("Processing song data")
    process_song_data(spark, input_data, output_data)
    print("Processing log data")
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
