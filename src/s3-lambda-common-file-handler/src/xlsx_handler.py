import pandas as pd
import json
import awswrangler as wr
import boto3
import os

def xlsx_handler(bucket,key):
    try:
        client = boto3.client(service_name='s3')
        # code for xlsx data processing
        filepath = f"s3://{bucket}/{key}"
        df = wr.s3.read_excel(filepath)
        df = df.drop_duplicates()
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
            'body': json.dumps('suceessfully converted xlsx file to csv and placed in qualified and also deleted it from raw bucket')
        }
    except Exception as e:
        print(e)
