import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, date_format, hour, year, month, weekofyear, dayofmonth, from_unixtime
from datetime import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates or get a Spark session and returns it.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    1. Extract song data from s3
    2. Load and Transform input it into Spark tables (Schema-on-Read)
    3. Store song and artists results into Parquet files
    Arguments:
        - spark: Spark session
        - input_data: s3 endpoint
        - output_data: s3 object name
    """
    staging_table = "staging_songs"
    spark\
        .read\
        .json("{}/song_data/*/*/*/".format(input_data))\
        .createOrReplaceTempView(staging_table)
    spark\
        .sql("SELECT DISTINCT song_id, title, artist_id, year, duration FROM {}".format(staging_table))\
        .write\
        .partitionBy("year", "artist_id")\
        .parquet("{}/songs.parquet".format(output_data), mode="Overwrite")

    spark\
        .sql("""
SELECT DISTINCT
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM {}
""".format(staging_table))\
        .write\
        .partitionBy("artist_id")\
        .parquet("{}/artists.parquet".format(output_data), mode="Overwrite")


def process_log_data(spark, input_data, output_data):
    """
    1. Extract log data from s3
    2. Load and Transform input it into Spark tables (Schema-on-Read)
    3. Store log results into Parquet files
    Arguments:
        - spark: Spark session
        - input_data: s3 endpoint
        - output_data: s3 object name
    """
    staging_log_table = "staging_logs"
    df = spark\
        .read\
        .json("{}/log_data/*/*/*/".format(input_data))
    df = df\
        .where(df.userId.isNotNull())\
        .where(df.userId != "")\
        .where(df.ts.isNotNull())\
        .where(df._corrupt_record.isNull())\
        .where(df.page == "NextSong")\
        .withColumn("timestamp", from_unixtime(col("ts")/1000))\
        .withColumn("hour", hour(from_unixtime(col("ts")/1000)))\
        .createOrReplaceTempView(staging_log_table)

    staging_song_table = "staging_songs"
    spark\
        .read\
        .json("{}/song_data/*/*/*/".format(input_data))\
        .createOrReplaceTempView(staging_song_table)
    spark\
        .sql("SELECT DISTINCT userId, firstName, lastName, gender,  level FROM {}".format(staging_log_table))\
        .write\
        .parquet("{}/users.parquet".format(output_data), mode="Overwrite")
    spark\
        .sql("""
SELECT DISTINCT
    ts as start_time,
    hour,
    dayofmonth(timestamp) as day,
    weekofyear(timestamp) as week,
    month(timestamp) as month,
    year(timestamp) as year,
    dayofweek(timestamp) as weekday
FROM {}
""".format(staging_log_table))\
        .write\
        .partitionBy("year", "month")\
        .parquet("{}/time.parquet".format(output_data), mode="Overwrite")
    spark\
        .sql("""
SELECT DISTINCT
    events.ts as start_time,
    month(events.timestamp) as month,
    year(events.timestamp) as year,
    events.userId as user_id,
    events.level,
    songs.song_id,
    songs.artist_id,
    events.sessionId as session_id,
    events.location,
    events.userAgent as user_agent
FROM {} events
JOIN staging_songs songs ON
    events.song = songs.title and events.artist = songs.artist_name
WHERE
    events.page = "NextSong"
""".format(staging_log_table, staging_song_table))\
        .write\
        .partitionBy("year", "month")\
        .parquet("{}/songplays.parquet".format(output_data), mode="Overwrite")


def main():
    spark = create_spark_session()
    input_data = config['S3_INPUT']
    output_data = config['S3_OUTPUT']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
