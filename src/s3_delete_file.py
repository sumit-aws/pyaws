import boto3
import os
import configparser


def initialize():
    s3_client = boto3.client(service_name='s3')
    return s3_client


def delete_file():
    target_bucket = read_config()
    all_objects = initialize().list_objects(Bucket=target_bucket)
    print(f"list of all objects to be deleted from {target_bucket}: ")
    for files in all_objects['Contents']:
        if 'example/' in files['Key'] and files['Key'] != 'example/':
            print(files['Key'])
            initialize().delete_object(
                Bucket=target_bucket,
                Key=files['Key']
            )


def read_config():
    config = configparser.RawConfigParser()
    config.read('conf/config.properties')
    qualified_bucket = config.get('dev', 'qualified_bucket')
    print(qualified_bucket)
    return qualified_bucket


if __name__ == "__main__":
    delete_file()