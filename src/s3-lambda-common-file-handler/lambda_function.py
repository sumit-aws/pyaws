import pandas as pd
import json
import awswrangler as wr
import boto3
import os
import urllib.parse
from src.json_handler import json_handler
from src.csv_handler import csv_handler
from src.xlsx_handler import xlsx_handler

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'],encoding='utf-8')
        print(key)
        filetype = os.path.splitext(key)[1]
        print(filetype)
        if filetype == '.csv':
            csv_handler(bucket,key)
        elif filetype == '.xlsx':
            xlsx_handler(bucket,key)
        elif filetype == '.json':
            json_handler(bucket,key)
    except Exception as e:
        raise e
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
