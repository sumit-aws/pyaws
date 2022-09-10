import pandas as pd
from datetime import date
import boto3
import os
import configparser

def initialize():
    s3_client = boto3.client(service_name='s3')
    return s3_client

def upload_file():
    filename= "data\sample_s3.yml"
    initialize().upload_file(filename, read_config(), "load_sample_001.yml")

def read_config():
    config = configparser.RawConfigParser()
    config.read('conf/config.properties')
    raw_bucket = config.get('dev', 'raw_bucket')
    print(raw_bucket)
    return raw_bucket

if __name__=="__main__":
    upload_file()