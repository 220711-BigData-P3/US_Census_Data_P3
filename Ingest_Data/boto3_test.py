
import zipfile
import boto3
import os

# import required module
import os

# need access key and secret key
session = boto3.Session(
    aws_access_key_id = '',
    aws_secret_access_key = ''
)

resource = session.resource('s3')
client = session.client('s3')

# uploading example file to s3 bucket:
myBucket = resource.Bucket('220711-bigdata-p3')
file = '2000_summaries_sorted.csv'
data = open(file, 'rb')
myBucket.put_object(Key=file, Body=data)

# testBucket = resource.Bucket('220711-pokemon-spencer')
# print(testBucket.objects.all())

# for item in testBucket.objects.all():
#     print(item.key)
#     with open('test.csv', 'wb') as f:
#         client.download_fileobj(item.bucket_name, item.key, f)