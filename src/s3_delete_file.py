import boto3
import os
from util.helper_functions import initialize,read_config


def delete_file():
    # code is for deleting the file
    target_bucket = read_config('qualified_bucket')
    all_objects = initialize('s3').list_objects(Bucket=target_bucket)
    print(f"list of all objects to be deleted from {target_bucket}: ")
    for files in all_objects['Contents']:
        if 'example/' in files['Key'] and files['Key'] != 'example/':
            print(files['Key'])
            initialize('s3').delete_object(
                Bucket=target_bucket,
                Key=files['Key']
            )


if __name__ == "__main__":
    delete_file()