import boto3
import os
import configparser


def initialize():
    # s3_client = boto3.client(service_name='s3')
    s3_client = boto3.resource('s3')
    return s3_client


def copy_file():
    copy_source = {
        'Bucket': read_config('raw_bucket'),
        'Key': 'orders_load_sample_001.csv'
    }

    target_bucket = initialize().Bucket(read_config('qualified_bucket'))
    target_bucket.copy(copy_source,'orders_load_sample_001_copied.csv')


def read_config(bucket):
    config = configparser.RawConfigParser()
    config.read('conf/config.properties')
    bucket = config.get('dev', bucket)
    print(bucket)
    return bucket


if __name__ == "__main__":
    copy_file()