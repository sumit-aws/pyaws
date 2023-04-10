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
        for i in event['Records']:
            s3_event = json.loads(i['body'])
            s3_event = json.loads(i['body'])
            if 'Event' in s3_event and s3_event['Event'] == 's3:TestEvent':
                print("Test Event")
            else:
                for j in s3_event['Records']:
                    bucket = j['s3']['bucket']['name']
                    key = j['s3']['object']['key']
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
