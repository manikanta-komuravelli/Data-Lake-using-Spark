import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
import pandas


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function is mainly used to create and utilise the Spark Session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    
    This function is used to load song_data from S3 and uses it by creating songs and artist tables
        and write the parquet files back  to S3
        
        Parameters Used:
            spark       : Spark Session
            input_data  : S3 location of the songs data
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    
    
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = "data/song_data/*/*/*/*.json"
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()

    # extract columns to create songs table
    song_table_list=["title", "artist_id","year", "duration"]
    songs_table = df.select(song_table_list).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/') 

    # extract columns to create artists table
    artist_table_list=["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.select(artist_table_list).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    
    """
    
    This function is used to load log_data from S3 and uses it create users, time and songplays tables.
    Load log files and input tables from S3, process data into table formats, and write these tables to partitioned parquet files on S3.
    Parameters: 
    spark - an active Spark session
    input_data - path to S3 bucket with input data
    output_data - path to S3 bucket to store output tables
  
    
    """
    # get filepath to log data file
    #log_data =input_data + "log_data/*.json"
    log_data= input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table= df.selectExpr(users_table_fields).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", to_timestamp(df.ts/1000))
    df = df.withColumn("Date", to_date(df.start_time))
    
    # extract columns to create time table
    time_table= df.selectExpr(['start_time as start_time',
                                'hour(Date) as hour',
                                'dayofmonth(Date) as day',
                                "weekofyear(Date) as week", 
                                "month(Date) as month", 
                                "year(Date) as year", 
                                "dayofweek(Date) as weekday"]).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/*/*/*')
    df_artists = spark.read.parquet(output_data + 'artists/*')
    songs_logs = df.join(song_df, (df.song == song_df.title))
    artists_songs_logs = songs_logs.join(df_artists, (songs_logs.artist == df_artists.artist_name))
    songplays = artists_songs_logs.join(time_table, artists_songs_logs.ts == time_table.start_time, 'left').drop(artists_songs_logs.year)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ).withColumn('songplay_id', monotonically_increasing_id()) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://aws-emr-resources-237696921313-us-west-2/datalake_output/"
    #input_data= "data/"
    #output_data = "/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
