import boto3
import pandas as pd
import json
import io

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:ap-south-1:471112572097:s3-lambda-sns-topic-1'

def lambda_handler(event, context):
    print(event)
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        print(bucket_name)
        print(s3_file_key)
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        print("response: ",response['Body'])
        data = response['Body'].read().decode('utf-8')
        json_data=json.loads(data)
        df = pd.json_normalize(json_data)
        print(df.head())
        df2 = df[df['status']=="delivered"]
        
        json_buffer = io.StringIO()
        df2.to_json(json_buffer)
        fileName = 'doordash-target-zn/2024-03-09-processed' + '.json '
        s3_client.put_object(Bucket=bucket_name, Key=fileName, Body=json_buffer.getvalue())
        
        # send the result to SNS topic
        message = "Input S3 File {} has been processed succesfuly !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
    except Exception as err:
        print(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')