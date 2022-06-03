import boto3
import configparser

def main():
    """
    Create bucket on S3 if not exists
    :return: None
    """
    config = configparser.ConfigParser()
    config.read_file(open('config.cfg'))

    KEY = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    SECRET = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    BUCKET_NAME = config.get('AWS', 'BUCKET_NAME')

    BUCKET_CONFIG = {'LocationConstraint': 'us-east-2'}

    s3_obj = boto3.client('s3', region_name="us-east-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

    try:
        s3_obj.create_bucket(Bucket=BUCKET_NAME,
                          CreateBucketConfiguration=BUCKET_CONFIG)
    except Exception as e:
        print(e)

    files = ["../Data/logs.json", "../Data/songs_data.json"]
    upload_files_to_s3(s3_obj, BUCKET_NAME, files)

def upload_files_to_s3(s3_obj, BUCKET_NAME, files):
    """
    Upload array of file using relative path to S3 Bucket

    Parameters:
    arg1 (S3 Client Object)
    arg2 (Bucket name)
    arg3 (array of file names)

    :return: None
    """
    for file in files:
        filename = file.split('/')[-1]
        print('File : {} to {}/{}'.format(file, BUCKET_NAME, filename))

        with open(file, 'rb') as data:
            s3_obj.put_object(Bucket=BUCKET_NAME, Key=filename, Body=data)

if __name__ == "__main__":
    main()
