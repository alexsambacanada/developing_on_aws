import boto3

s3 = boto3.resource('s3')
s3Client = boto3.client('s3')

bucket_name = 'demo-aai-developing-on-aws-alexw'
object_key = 'sampledata.csv'
file_to_upload = 'sampledata.csv'
downloaded_file_path = './sampledata.csv'

import os
os.chdir('/home/ec2-user/environment/developing_on_aws/Module_6/')
cwd = os.getcwd()
print(cwd)

    
#Create Bucket
def create_s3_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' creation request sent.")
        
        # Create a waiter for bucket existence
        bucket = s3.Bucket(bucket_name)
        bucket.wait_until_exists()
        
        print(f"Bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create bucket '{bucket_name}': {str(e)}")
    
#Put an object in the bucket
def put_object_in_bucket(bucket_name, object_key, file_path):
    try:
        s3.Bucket(bucket_name).upload_file(file_path, object_key)
        print(f"Object '{object_key}' uploaded to '{bucket_name}' successfully.")
    except Exception as e:
        print(f"Failed to upload object '{object_key}' to '{bucket_name}': {str(e)}")

#Get Object Metadata
def get_object_metadata(bucket_name, object_key):
    try:
        metadata = s3Client.head_object(Bucket=bucket_name, Key=object_key)
        
        print(metadata)
    except Exception as e:
        print(f"Failed to retrieve object metadata for '{object_key}' in bucket '{bucket_name}': {str(e)}")

    
#Get an Object
def download_object_from_bucket(bucket_name, object_key, destination_path):
    try:
        s3.Bucket(bucket_name).download_file(object_key, destination_path)
        print(f"Object '{object_key}' downloaded to '{destination_path}' successfully.")
    except Exception as e:
        print(f"Failed to download object '{object_key}' from '{bucket_name}': {str(e)}")


#Delete Bucket
def delete_bucket(bucket_name):
    try:
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.all():
            obj.delete()
        bucket.delete()
        print(f"Bucket '{bucket_name}' emptied and deleted successfully.")
    except Exception as e:
        print(f"Failed to empty and delete bucket '{bucket_name}': {str(e)}")
    

create_s3_bucket(bucket_name)
input("Press Enter to continue...")

put_object_in_bucket(bucket_name, object_key, file_to_upload)
input("Press Enter to continue...")

get_object_metadata(bucket_name, object_key)
input("Press Enter to exit...")

download_object_from_bucket(bucket_name, object_key, downloaded_file_path)
input("Press Enter to exit...")

delete_bucket(bucket_name)
input("Press Enter to exit...")
    
