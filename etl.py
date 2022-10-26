import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create spark session 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    create songs table
    create artists table
    write songs table to parquet files partitioned by year and artist
    write artists table to parquet files
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """
    create user table
    create time_table 
    create songplays_table
    write time table to parquet files partitioned by year and month
    write songplays table to parquet files partitioned by year and month
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    users_table = users_table.drop_duplicates(subset=['userId'])
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    
    # extract columns to create time table
    time_table =  df.select(
                F.col("timestamp").alias("start_time"),
                F.hour("timestamp").alias('hour'),
                F.dayofmonth("timestamp").alias('day'),
                F.weekofyear("timestamp").alias('week'),
                F.month("timestamp").alias('month'), 
                F.year("timestamp").alias('year'), 
                F.date_format(F.col("timestamp"), "E").alias("weekday")
            )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(
    song_df,
    (df.song == song_df.title) & 
    (df.artist == song_df.artist_name) &
    (df.length == song_df.duration) &
    (year(df.timestamp) == song_df.year),
    'left_outer').select(
    df.timestamp.alias("start_time"),
    df.userId.alias("user_id"),
    df.level,
    song_df.song_id,
    song_df.artist_id,
    df.sessionId.alias("session_id"),
    df.location,
    df.userAgent.alias("user_agent"),
    year(df.timestamp).alias('year'),
    month(df.timestamp).alias('month')).orderBy("start_time", "user_id").withColumn("songplay_id", F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main():
    """
    e
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://ahmed-datalake-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
