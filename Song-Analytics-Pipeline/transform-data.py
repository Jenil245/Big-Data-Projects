# Databricks notebook source
# Get AWS credentials from csv file

# Define file read options
file_type = "csv"
first_row_is_header = "true"
delimiter = ","

AWS_credentials = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("dbfs:/FileStore/shared_uploads/jenil2452000@gmail.com/song_project_credentials.csv")

# COMMAND ----------

AWS_ACCESS_KEY = AWS_credentials.collect()[0]['Access key ID']
AWS_SECRET_KEY = AWS_credentials.collect()[0]['Secret access key']
#ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME="song-analysis"
MOUNT_NAME = "/mnt/song-data"

# COMMAND ----------

dbutils.fs.unmount("/mnt/song-data")   # to unmount the s3 bucket
SOURCE_URL = "s3a://{0}:{1}@{2}".format(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_BUCKET_NAME)# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# File location and type
file_location = "/mnt/song-data/songs_data.json"
file_type = "json"  #json options
infer_schema = "true"

df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)

# COMMAND ----------

df.head(5)

# COMMAND ----------


