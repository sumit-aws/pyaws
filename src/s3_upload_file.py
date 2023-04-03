import pandas as pd
from datetime import date
import boto3
import os
import configparser


def initialize():
    s3_client = boto3.client(service_name='s3')
    return s3_client


def upload_file():
    filename = "data\sample_s3.yml"
    initialize().upload_file(filename, read_config('raw_bucket'), "load_sample_001.yml")


def read_config(bucket):
    config = configparser.RawConfigParser()
    config.read('conf/config.properties')
    bucket = config.get('dev', bucket)
    print(bucket)
    return bucket


if __name__ == "__main__":
    upload_file()
