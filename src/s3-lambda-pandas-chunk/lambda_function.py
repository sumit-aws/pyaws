import pandas as pd
import json
import awswrangler as wr
import boto3
import os
import urllib.parse

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'],encoding='utf-8')
        filepath = f"s3://{bucket}/{key}"
        chunk = wr.s3.read_csv(filepath,chunksize=10000)
        df = pd.concat(chunk)
        df = df.drop_duplicates()
        file = os.path.splitext(key)[0]+"_001_processed.csv"
        target_bucket = os.environ["target_bucket"]
        wr.s3.to_csv(df, f"s3://{target_bucket}/{file}")
    except Exception as e:
        raise e
    return {
        'statusCode': 200,
        'body': json.dumps('successfully read huge csv file in chunks')
    }
