import boto3
import os
from dotenv import load_dotenv

load_dotenv()

access_key = os.getenv("ACCESS_KEY_ID")
secret_access_key = os.getenv("SECRET_ACCESS_KEY")
bucket_name = os.getenv("BUCKET_NAME")
region_name = os.getenv("REGION_NAME")

session = boto3.Session(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_access_key,
    region_name = region_name
)

resource = session.resource('s3')
client = session.client('s3')


#fetch bucket from resource
bucket = resource.Bucket(bucket_name)


################S3 DOWNLOAD, CHECK CONTENTS, DELETION FUNCTIONS HERE!####################


def download_from_s3(bucket):
    #Downloads file into local
    for item in bucket.objects.all():
        with open(item.key, 'wb') as f:
            client.download_fileobj(item.bucket_name, item.key, f)
            
            
def check_bucket_contents(bucket):
    bucket_objects = bucket.objects.all()
    names = map(lambda summary: summary.key, bucket_objects)
    return names
        
def delete_file_from_bucket(bucketname, bucket, filename):
    names = check_bucket_contents(bucket)
    if filename not in names:
        print(f"Error: File: {filename} doesn't exist.")
        return 
    resource.Object(bucketname, filename).delete()
    print(f"Succesfully deleted {filename}!")
    return 


check_bucket_contents(bucket)
delete_file_from_bucket(bucket_name, bucket,'tgjksdyhfgkldshjfg', )
check_bucket_contents(bucket)
#download_from_s3(bucket)
