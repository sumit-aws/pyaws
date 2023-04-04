import awswrangler as wr
import os
from util.helper_functions import initialize,read_config


def csv_process():
    # code for xlsx data processing
    filepath = f"s3://{read_config('raw_bucket')}/peoples.xlsx"
    df = wr.s3.read_excel(filepath)
    df = df.drop_duplicates()
    print(df.head())
    wr.s3.to_csv(df, f"s3://{read_config('qualified_bucket')}/peoples_001_processed.csv")

    # code for deleting file from raw bucket after it processed
    target_bucket = read_config('raw_bucket')
    filename = os.path.basename(filepath)
    print(filename)
    initialize('s3').delete_object(
                Bucket=target_bucket,
                Key=filename
            )

if __name__ == "__main__":
    csv_process()