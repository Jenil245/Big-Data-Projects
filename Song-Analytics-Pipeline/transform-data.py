# Databricks notebook source
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType, LongType, DoubleType
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

AWS_credentials = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("dbfs:/FileStore/shared_uploads/jenil2452000@gmail.com/song_project_credentials.csv")

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
    
    songs_df = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    artists_df = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])\
                    .withColumnRenamed('artist_name','name') \
                    .withColumnRenamed('artist_location','location') \
                    .withColumnRenamed('artist_latitude','latitude') \
                    .withColumnRenamed('artist_longitude','longitude').dropDuplicates()
    
    return df, songs_df, artists_df

# COMMAND ----------

def process_logs_dataframe(spark, df):
    '''
    Process logs dataframe to organize the tables and clean data
    
    Parameters:
    arg1 (Spark): Spark object
    arg2 (DataFrame): logs dataframe
    
    Returns (DataFrames): Returning organized dataframes
    '''

    paid_users = df.select(['userId', 'level']).filter( df['level'] == 'paid')
    paid_users = paid_users.groupby(['userId']).count()
    free_users = df.select(['userId', 'level']).filter( df['level'] == 'free')
    free_users = free_users.groupby(['userId']).count()
    paid_users.createOrReplaceTempView('paid_users')
    free_users.createOrReplaceTempView('free_users')
    user_listen = spark.sql("""
        select a.userId, a.count puCount, b.count fuCount
        from paid_users a join free_users b
        on a.userId = b.userId
        where a.userId != ''
    """)        
    
    print(user_listen.show())
    
    

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

original_songs_df, songs_df, artists_df = process_songs_dataframe(spark, songs_df)

# COMMAND ----------

process_logs_dataframe(spark, logs_df)

# COMMAND ----------


