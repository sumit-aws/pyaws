import boto3
import os
import configparser

def initialize(service):
    client = boto3.client(service_name=service)
    return client

def read_config(bucket):
    config = configparser.RawConfigParser()
    config.read('conf/config.properties')
    bucket = config.get('dev', bucket)
    print(bucket)
    return bucket