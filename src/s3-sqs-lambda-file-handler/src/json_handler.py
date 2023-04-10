import pandas as pd
import json
import awswrangler as wr
import boto3
import os

def json_handler(bucket,key):
    try:
        client = boto3.client(service_name='s3')
        response = client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read().decode('utf-8')
        json_data=json.loads(data)
        df = pd.json_normalize(json_data,record_path='experience',errors='ignore',meta=['empId','empName','salary','machineId'])
        
        df = df[['empId','empName','salary','machineId','role','roleExperience']]
        print(df.head())
    
        # code to write pandas dataframe to csv file in s3 bucket using awswrangler
        file = os.path.splitext(key)[0]+"_001_processed.csv"
        wr.s3.to_csv(df, f"s3://sumit-aws-qualified/{file}")
        target_bucket = bucket
        filename = key
        client.delete_object(
                    Bucket=target_bucket,
                    Key=filename
                )
        return {
            'statusCode': 200,
            'body': json.dumps('JSON file successfuly converted to csv!')
        }
    except Exception as e:
        raise e
