import boto3
from botocore.exceptions import ClientError

# Create a session without assuming any role
session = boto3.Session(region_name='us-east-1')

# Create an STS client to assume the "monkey" IAM role
sts_client = session.client('sts')

# Specify the IAM role ARN you want to assume
role_arn = 'arn:aws:iam::475815351148:role/developing-on-aws-mod-kinesis-ro'

try:
    
    
    # # Assuming the IAM role
    # response = sts_client.assume_role(
    #     RoleArn=role_arn,
    #     RoleSessionName='AssumeKinesisRoleSession'
    # )
    
    # print(f"Temp Access Key ID: {response['Credentials']['AccessKeyId']}")
    # print(f"Temp Secret Access Key ID: {response['Credentials']['SecretAccessKey']}")
    # print(f"Expiration: {response['Credentials']['Expiration']}")

    # # Extract temporary credentials
    # credentials = response['Credentials']

    # # Create a new session using the temporary credentials
    # session = boto3.Session(
    #     aws_access_key_id=credentials['AccessKeyId'],
    #     aws_secret_access_key=credentials['SecretAccessKey'],
    #     aws_session_token=credentials['SessionToken'],
    #     region_name='us-east-1'
    # )

    # Create a Kinesis client using the new session
    kinesis_client = session.client('kinesis')

    # List Kinesis streams
    response = kinesis_client.list_streams()

    # Print the list of Kinesis streams
    print("Kinesis Streams:")
    for stream_name in response['StreamNames']:
        print(stream_name)

except ClientError as e:
    if e.response['Error']['Code'] == 'AccessDeniedException':
        print("Access to Kinesis is denied. Check IAM permissions.")
    else:
        print(f"An error occurred: {e}")
