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
        .builder \
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
AWS_BUCKET_NAME="raw-song-data"
MOUNT_PATH = "/mnt/song-data/"

# COMMAND ----------

# mount the s3 bucket

dbutils.fs.unmount("/mnt/song-data")   # to unmount the s3 bucket
SOURCE_URL = "s3a://{0}:{1}@{2}".format(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_BUCKET_NAME)# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_PATH)

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
    users_df = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])\
            .withColumnRenamed('userId', 'user_id')\
            .withColumnRenamed('firstName', 'first_name')\
            .withColumnRenamed('lastName', 'last_name').dropDuplicates()
    
    users_df.createOrReplaceTempView('users')    
    
    
    # create table that has information on users who have used paid and free version to listen songs
    paid_users = df.select(['userId', 'level']).filter( df['level'] == 'paid')
    paid_users = paid_users.groupby(['userId']).count()
    free_users = df.select(['userId', 'level']).filter( df['level'] == 'free')
    free_users = free_users.groupby(['userId']).count()
    
    paid_users.createOrReplaceTempView('paid_users')
    free_users.createOrReplaceTempView('free_users')
    
    user_level_listen_df = spark.sql("""
        select a.userId user_id, a.count paid_use_count, b.count free_use_count
        from paid_users a join free_users b
        on a.userId = b.userId
        where a.userId != ''
    """)      
    
    user_level_listen_df.createOrReplaceTempView('user_level_listen')
    
    user_level_listen_df = spark.sql("""
        select distinct u.first_name || ' ' || u.last_name as full_name, ul.paid_use_count, ul.free_use_count
        from users u join user_level_listen ul
        on u.user_id = ul.user_id
    """)
    
    # create a table that has all the time stamp information for deeper analytics
    time_df = df.select(['ts_converted']).withColumnRenamed('ts_converted','start_time') 

    time_df = time_df.withColumn('day', F.dayofmonth('start_time')) \
                          .withColumn('month', F.month('start_time')) \
                          .withColumn('year', F.year('start_time')) \
                          .withColumn('hour', F.hour('start_time')) \
                          .withColumn('minute', F.minute('start_time')) \
                          .withColumn('second', F.second('start_time')) \
                          .withColumn('week', F.weekofyear('start_time')) \
                          .withColumn('weekday', F.dayofweek('start_time')).dropDuplicates()   
    
    return df, users_df, user_level_listen_df, time_df   

# COMMAND ----------

songs_file_location = MOUNT_PATH+"songs_data.json"
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

logs_file_location = MOUNT_PATH+"logs.json"
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

modified_songs_df.cache()

# COMMAND ----------

modified_logs_df, users_df, user_level_listen_df, time_df = process_logs_dataframe(spark, logs_df)

modified_logs_df.cache()

# COMMAND ----------

# check whether storage level is set to in-memory or not

print(modified_songs_df.storageLevel.useMemory)
print(modified_logs_df.storageLevel.useMemory)

# COMMAND ----------

def process_songplays(spark, logs_df, songs_df, artists_df, users_df, time_df):
    """
    Summary line. 
    Process songplays
  
    Parameters: 
    arg1 (Spark): (spark object)
    arg2 (DataFrame): logs dataframe
    arg3 (DataFrame): songs dataframe
    arg4 (DataFrame): artists dataframe
    arg5 (DataFrame): users dataframe
    arg6 (DataFrame): time dataframe
  
    Returns(DataFrame): song plays dataframe
    """            
    
    # Creating tables
    logs_df.createOrReplaceTempView('logs')
    songs_df.createOrReplaceTempView('songs')
    artists_df.createOrReplaceTempView('artists')
    users_df.createOrReplaceTempView('users')
    time_df.createOrReplaceTempView('time')
    
    
    songplays_df = spark.sql("""
    select  t.start_time, l.userId as user_id, l.level, s.song_id, a.artist_id, l.sessionId as session_id, l.location, l.userAgent as user_agent
    , t.year, t.month
    from logs l join time t
    on l.ts_converted = t.start_time
    join artists a
    on l.artist = a.name
    join songs s
    on l.song = s.title
    """)

    # create a monotonically increasing id 
    # A column that generates monotonically increasing 64-bit integers.
    # The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    songplays_df = songplays_df.withColumn("idx", F.monotonically_increasing_id())

    # Then since the id is increasing but not consecutive, it means you can sort by it, so you can use the `row_number`
    songplays_df.createOrReplaceTempView('songplays_table')
    songplays_df = spark.sql("""
    select row_number() over (order by "idx") as num, 
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month
    from songplays_table
    """)
 
    return songplays_df       

# COMMAND ----------

songplays_df = process_songplays(spark, modified_logs_df, songs_info_df, artists_df, users_df, time_df)

# COMMAND ----------

print(songplays_df.count())
songplays_df.show(5)

# COMMAND ----------


