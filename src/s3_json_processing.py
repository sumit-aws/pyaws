import pandas as pd
import json
import awswrangler as wr
from util.helper_functions import initialize,read_config


def json_process():
    # code to read json data using pandas
    response = initialize('s3').get_object(Bucket=read_config('raw_bucket'), Key="employee.json")
    data = response['Body'].read().decode('utf-8')
    json_data=json.loads(data)
    df = pd.json_normalize(json_data,record_path='experience',errors='ignore',meta=['empId','empName','salary','machineId'])
    
    df = df[['empId','empName','salary','machineId','role','roleExperience']]
    print(df.head())

    # code to write pandas dataframe to csv file in s3 bucket using awswrangler
    wr.s3.to_csv(df, f"s3://{read_config('qualified_bucket')}/employee_load_sample_001_processed.csv")

if __name__ == "__main__":
    json_process()