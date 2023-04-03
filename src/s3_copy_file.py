import boto3
import os
from util.helper_functions import read_config


def initialize(service):
    # s3_client = boto3.client(service_name='s3')
    client = boto3.resource('s3')
    return client


def copy_file():
    copy_source = {
        'Bucket': read_config('raw_bucket'),
        'Key': 'orders_load_sample_001.csv'
    }

    target_bucket = initialize('s3').Bucket(read_config('qualified_bucket'))
    target_bucket.copy(copy_source,'orders_load_sample_001_copied.csv')


if __name__ == "__main__":
    copy_file()