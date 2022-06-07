# Databricks notebook source
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType, LongType, DoubleType
import datetime #Required for ts conversion
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second

# COMMAND ----------

def create_spark_session():
    """
    Define spark session
  
    Parameters: None
  
    Returns: 
    spark session object
    """     
    
    spark = SparkSession \
        .builder() \
        .appName("Data-Transformation") \
        .getOrCreate()
    
    return spark

# COMMAND ----------

# Get AWS credentials from csv file

# Define file read options
file_type = "csv"
first_row_is_header = "true"
delimiter = ","
file_location = "dbfs:/FileStore/shared_uploads/jenil2452000@gmail.com/song_project_credentials.csv"

AWS_credentials = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load(file_location)

#Set up the credential variables
AWS_ACCESS_KEY = AWS_credentials.collect()[0]['Access key ID']
AWS_SECRET_KEY = AWS_credentials.collect()[0]['Secret access key']
#ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME="song-analysis"
MOUNT_NAME = "/mnt/song-data"

# COMMAND ----------

# mount the s3 bucket

dbutils.fs.unmount("/mnt/song-data")   # to unmount the s3 bucket
SOURCE_URL = "s3a://{0}:{1}@{2}".format(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_BUCKET_NAME)# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

def read_s3_data(spark, file_location, file_type, schema):
    '''
    Read S3 data into a dataframe given location, type, and schema of the file with spark object
    
    Paramters: 
    arg1 (Spark): Spark object
    arg2 (str): Location of file
    arg3 (str): Type of file
    arg4 (Struct): Schema of the data
    
    Returns (DataFrame): Dataframe of the data
    '''
    
    df = spark.read.format(file_type) \
    .schema(schema) \
    .load(file_location)
    
    return df

# COMMAND ----------

def process_songs_dataframe(spark, df):
    '''
    Process songs dataframe to organize the tables and clean data
    
    Parameters:
    arg1 (Spark): Spark object
    arg2 (DataFrame): songs dataframe
    
    Returns (DataFrames): Returning organized artist and songs dataframe
    '''
    
    # replacing null values
    df = df.fillna({'artist_latitude':0})
    df = df.fillna({'artist_longitude':0})
    
    songs_info_df = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    artists_df = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])\
                    .withColumnRenamed('artist_name','name') \
                    .withColumnRenamed('artist_location','location') \
                    .withColumnRenamed('artist_latitude','latitude') \
                    .withColumnRenamed('artist_longitude','longitude').dropDuplicates()
    
    return df, songs_info_df, artists_df

# COMMAND ----------

def process_logs_dataframe(spark, df):
    '''
    Process logs dataframe to organize the tables and clean data
    
    Parameters:
    arg1 (Spark): Spark object
    arg2 (DataFrame): logs dataframe
    
    Returns (DataFrames): Returning organized dataframes
    '''
    
    # only take users who are listening to music continuously
    df = df.filter(df['page']=='NextSong')
    
    # Convert ts from long to datetime
    # Timestamp is in  milliseconds so we need to divide it by 1000 otherwise year out of range error will be displayed
    convert_time = udf(lambda x: datetime.datetime.fromtimestamp(float(x) / 1000.0), TimestampType())
    df = df.withColumn("ts_converted", convert_time(df.ts))
    
    # Convert registration from double to long
    df = df.withColumn("registration_converted", df.registration.cast(LongType()))
    
    # creating users table with useful information like full name, gender, level
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])\
            .withColumnRenamed('userId', 'user_id')\
            .withColumnRenamed('firstName', 'first_name')\
            .withColumnRenamed('lastName', 'last_name').dropDuplicates()
    
    users_table.createOrReplaceTempView('users')    
    
    
    # create table that has information on users who have used paid and free version to listen songs
    paid_users = df.select(['userId', 'level']).filter( df['level'] == 'paid')
    paid_users = paid_users.groupby(['userId']).count()
    free_users = df.select(['userId', 'level']).filter( df['level'] == 'free')
    free_users = free_users.groupby(['userId']).count()
    
    paid_users.createOrReplaceTempView('paid_users')
    free_users.createOrReplaceTempView('free_users')
    
    user_level_listen = spark.sql("""
        select a.userId user_id, a.count paid_use_count, b.count free_use_count
        from paid_users a join free_users b
        on a.userId = b.userId
        where a.userId != ''
    """)      
    
    user_level_listen.createOrReplaceTempView('user_level_listen')
    
    user_level_listen = spark.sql("""
        select distinct u.first_name || ' ' || u.last_name as full_name, ul.paid_use_count, ul.free_use_count
        from users u join user_listen ul
        on u.user_id = ul.user_id
    """)
    
    # create a table that has all the time stamp information for deeper analytics
    timestamp_table = df.select(['ts_converted']).withColumnRenamed('ts_converted','start_time') 

    timestamp_table = timestamp_table.withColumn('day', F.dayofmonth('start_time')) \
                          .withColumn('month', F.month('start_time')) \
                          .withColumn('year', F.year('start_time')) \
                          .withColumn('hour', F.hour('start_time')) \
                          .withColumn('minute', F.minute('start_time')) \
                          .withColumn('second', F.second('start_time')) \
                          .withColumn('week', F.weekofyear('start_time')) \
                          .withColumn('weekday', F.dayofweek('start_time')).dropDuplicates()   
    
    return df, users_table, user_level_listen, timestamp_table   

# COMMAND ----------

songs_file_location = "/mnt/song-data/songs_data.json"
songs_file_type = "json"

song_schema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", FloatType()),
        StructField("artist_longitude", FloatType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", FloatType()),
        StructField("year", IntegerType())
    ])

songs_df = read_s3_data(spark, songs_file_location, songs_file_type, song_schema)
songs_df.head(5)

# COMMAND ----------

logs_file_location = "/mnt/song-data/logs.json"
logs_file_type = "json"

log_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", LongType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", LongType()),
        StructField("song", StringType()),
        StructField("status", StringType()),
        StructField("ts", StringType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())
    ])

logs_df = read_s3_data(spark, logs_file_location, logs_file_type, log_schema)
logs_df.head(5)

# COMMAND ----------

modified_songs_df, songs_info_df, artists_df = process_songs_dataframe(spark, songs_df)

# COMMAND ----------

modified_logs_df, users_table, user_level_listen, timestamp_table = process_logs_dataframe(spark, logs_df)
