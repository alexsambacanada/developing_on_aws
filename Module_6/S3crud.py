import boto3

s3 = boto3.resource('s3')

bucket_name = 'alexalexalexyeahyeahyeah'
object_key = 'blah.txt'
file_to_upload = './blah.txt'
downloaded_file_path = './blahdownloaded.txt'


def create_s3_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create bucket '{bucket_name}': {str(e)}")
    


def put_object_in_bucket(bucket_name, object_key, file_path):
    try:
        s3.Bucket(bucket_name).upload_file(file_path, object_key)
        print(f"Object '{object_key}' uploaded to '{bucket_name}' successfully.")
    except Exception as e:
        print(f"Failed to upload object '{object_key}' to '{bucket_name}': {str(e)}")
    


def download_object_from_bucket(bucket_name, object_key, destination_path):
    try:
        s3.Bucket(bucket_name).download_file(object_key, destination_path)
        print(f"Object '{object_key}' downloaded to '{destination_path}' successfully.")
    except Exception as e:
        print(f"Failed to download object '{object_key}' from '{bucket_name}': {str(e)}")
        
def get_object_metadata(bucket_name, object_key):
    try:
        obj = s3.Object(bucket_name, object_key)
        metadata = obj.metadata
        print(f"Metadata for object '{object_key}': {metadata}")
    except Exception as e:
        print(f"Failed to retrieve metadata for object '{object_key}': {str(e)}")
        
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
input("Press Enter to continue...")

delete_bucket(bucket_name)
input("Press Enter to exit...")
    
