import boto3
import os
from util.helper_functions import initialize,read_config

def list_objects():
    # code to print all objects in perticular target_bucket
    target_bucket = read_config('qualified_bucket')
    all_objects = initialize('s3').list_objects(Bucket=target_bucket)
    print(f"list of all objects in {target_bucket}: ")
    for files in all_objects['Contents']:
        print(files['Key'])
    
    # code to print all available bucket names in s3 account
    response = initialize('s3').list_buckets()
    print(f"list of all buckets in s3 account: ")
    for buckets in response['Buckets']:
        print(buckets['Name'])


if __name__ == "__main__":
    list_objects()