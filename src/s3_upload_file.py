import boto3
import os
from util.helper_functions import initialize,read_config


def upload_file():
    filename = "data\sample_s3.yml"
    initialize('s3').upload_file(filename, read_config('raw_bucket'), "load_sample_001.yml")


if __name__ == "__main__":
    upload_file()
