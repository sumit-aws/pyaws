import boto3
import os
import configparser


def initialize():
    s3_client = boto3.client(service_name='s3')
    return s3_client


def delete_file():
    target_bucket = read_config('qualified_bucket')
    all_objects = initialize().list_objects(Bucket=target_bucket)
    print(f"list of all objects to be deleted from {target_bucket}: ")
    for files in all_objects['Contents']:
        if 'example/' in files['Key'] and files['Key'] != 'example/':
            print(files['Key'])
            initialize().delete_object(
                Bucket=target_bucket,
                Key=files['Key']
            )


def read_config(bucket):
    config = configparser.RawConfigParser()
    config.read('conf/config.properties')
    bucket = config.get('dev', bucket)
    print(bucket)
    return bucket


if __name__ == "__main__":
    delete_file()