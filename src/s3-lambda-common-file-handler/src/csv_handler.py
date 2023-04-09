import pandas as pd
import json
import awswrangler as wr
import boto3
import os

def csv_handler(bucket,key):
    try:
        client = boto3.client(service_name='s3')
        # code for csv data processing
        filepath = f"s3://{bucket}/{key}"
        df = wr.s3.read_csv(filepath)
        df = df.drop_duplicates()
        df = df[df['product_id'].str.contains('GTX')]
        print(df.head())
        file = os.path.splitext(key)[0]+"_001_processed.csv"
        wr.s3.to_csv(df, f"s3://sumit-aws-qualified/{file}")
    
        # code for deleting file from raw bucket after it processed
        target_bucket = bucket
        filename = os.path.basename(filepath)
        print(filename)
        client.delete_object(
                    Bucket=target_bucket,
                    Key=filename
                )
        return {
        'statusCode': 200,
        'body': json.dumps('csv processed successfuly')
    }
    except Exception as e:
        raise e
    