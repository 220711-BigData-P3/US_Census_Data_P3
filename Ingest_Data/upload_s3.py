import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

def upload_file_s3(file_name, object_name=None):
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

    client = session.client('s3')
    """
    Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        response = client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        print(e)
        return False
    return True