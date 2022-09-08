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

#path
sliced = os.path.dirname(__file__).split('Us_Census_Data_P3')
root_path = sliced[0]+'Us_Census_Data_P3'
bucket = resource.Bucket(bucket_name)

def main():
    #fetch bucket from resource
    
    
    program_running = True
    while program_running == True:
        print("S3 Bucket Retrieval Program:")
        print("input the number of your selection")
        print(f"1. Download files into root directory: {root_path}")
        print("2. Check S3 Bucket Contents")
        print("3. Delete file from S3 Bucket")
        print("q to exit")
        user_input = input()
        match user_input:
            case "1":
                download_from_s3(bucket, root_path)
            case "2":
                bucket_files = check_bucket_contents(bucket)
                print(bucket_files)
            case "3":
                check_bucket_contents(bucket)
                print("Enter file name:")
                filename = input().strip()
                delete_file_from_bucket(bucket_name, bucket, filename)
            case default:
                print('Exiting...')
                return
                
                
            
        


################S3 DOWNLOAD, CHECK CONTENTS, DELETION FUNCTIONS HERE!####################
def local_fs_contents(root_path):   
    return os.listdir(root_path)
    

def download_from_s3(bucket, root_path):
    root_files = local_fs_contents(root_path)
    #Downloads file into local
    for item in bucket.objects.all():
        if item.key in root_files:
            print(f"The file {item.key} already exists in the root directory. Would you like to overwrite? y/n")
            answer = input()
            if answer.lower() == 'y':
                with open(root_path+'\\'+item.key, 'wb') as f1:
                    print(root_path+'\\'+item.key)
                    client.download_fileobj(item.bucket_name, item.key, f1)
                print(f'Successful Overwrite: {item.key}')
            else:
                continue
        else:   
            with open(root_path+'\\'+item.key, 'wb') as f2:
                print(root_path+'\\'+item.key)
                client.download_fileobj(item.bucket_name, item.key, f2)
            print(f'Successful Download {item.key}')
            
            
            
def check_bucket_contents(bucket):
    bucket_objects = bucket.objects.all()
    names = map(lambda summary: summary.key, bucket_objects)
    return list(names)
        
def delete_file_from_bucket(bucketname, bucket, filename):
    names = check_bucket_contents(bucket)
    if filename not in names:
        print(f"Error: File: {filename} doesn't exist.")
        return 
    resource.Object(bucketname, filename).delete()
    print(f"Succesfully deleted {filename}!")
    return 

main()