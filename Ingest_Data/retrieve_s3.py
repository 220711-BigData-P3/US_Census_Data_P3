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


#fetch
bucket = resource.Bucket(bucket_name)
#print
#Download file into local
for item in bucket.objects.all():
    print(item.key)
    with open(item.key, 'wb') as f:
        client.download_fileobj(item.bucket_name, item.key, f)