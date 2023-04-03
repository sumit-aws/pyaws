import boto3
import os
import configparser


def initialize():
    # s3_client = boto3.client(service_name='s3')
    s3_client = boto3.resource('s3')
    return s3_client


def copy_file():
    copy_source = {
        'Bucket': 'sumit-aws-raw',
        'Key': 'load_sample_001.yml'
    }

    target_bucket = initialize().Bucket(read_config())
    target_bucket.copy(copy_source,'load_sample_001_copied.yml')


def read_config():
    config = configparser.RawConfigParser()
    config.read('conf/config.properties')
    qualified_bucket = config.get('dev', 'qualified_bucket')
    print(qualified_bucket)
    return qualified_bucket


if __name__ == "__main__":
    copy_file()