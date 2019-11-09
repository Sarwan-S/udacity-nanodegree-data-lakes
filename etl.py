import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Description: Function to create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Description: Function to process song data files from S3 into dimension tables and write them back to output_data location
    Inputs: 
        spark: spark session object
        input_data: S3 bucket location of staging files
        output_data: S3 bucket location to save processed files to
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("df")

    # extract columns to create songs table
    songs_table = spark.sql("""
                                SELECT DISTINCT 
                                       song_id, 
                                       title, 
                                       artist_id, 
                                       year, 
                                       duration
                                FROM df
                                WHERE song_id IS NOT NULL
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT 
                                       artist_id, 
                                       artist_name as name, 
                                       artist_location as location, 
                                       artist_longitude as latitude, 
                                       artist_latitude as longitude
                                FROM df
                                WHERE artist_id IS NOT NULL
                              """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Description: Function to process log data files from S3 into fact and dimension tables and write them back to output_data location
    Inputs: 
        spark: spark session object
        input_data: S3 bucket location of staging files
        output_data: S3 bucket location to save processed files to
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page = 'NextSong'")
    df.createOrReplaceTempView("df")

    # extract columns for users table    
    users_table = spark.sql("""
                                SELECT DISTINCT
                                       userId as user_id,
                                       firstName as first_name,
                                       lastName as last_name,
                                       gender,
                                       level,
                                       ROW_NUMBER() over (PARTITION BY userId ORDER BY ts DESC) as row_num
                                FROM df
                                WHERE userId IS NOT NULL
                            """)
    users_table = users_table.where("row_num = 1").drop("row_num")
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    df.createOrReplaceTempView("df")
    
    # extract columns to create time table
    time_table = spark.sql("""
                           SELECT DISTINCT
                                  timestamp as start_time, 
                                  hour(timestamp) as hour, 
                                  day(timestamp) as day, 
                                  weekofyear(timestamp) as week, 
                                  month(timestamp) as month, 
                                  year(timestamp) as year, 
                                  dayofweek(timestamp) as weekday
                            FROM df
                           """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")
    song_df.createOrReplaceTempView("song_df")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT DISTINCT
                                       df.timestamp as start_time,
                                       df.userId as user_id,
                                       df.level,
                                       song_df.song_id,
                                       song_df.artist_id,
                                       df.sessionId as session_id,
                                       df.location,
                                       df.userAgent as user_agent,
                                       month(df.timestamp) as month, 
                                       year(df.timestamp) as year 
                                FROM df JOIN song_df
                                ON df.artist = song_df.artist_name
                                AND df.song = song_df.title
                                AND df.length = song_df.duration
                                """)
    
    #create column for songplay_id
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    #re-arrange columns
    songplays_table = songplays_table.select("songplay_id", "start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "month", "year")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    """
    Description: main function for running both the process_song_data and process_log_data functions
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "" #e.g "s3a://my-s3-bucketname/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
