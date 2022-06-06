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

def process_songs_data(spark, df):
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

original_songs_df, songs_df, artists_df = process_songs_data(spark, songs_df)
