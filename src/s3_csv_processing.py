import awswrangler as wr
import os
from util.helper_functions import initialize,read_config


def csv_process():
    # code for csv data processing
    filepath = f"s3://{read_config('raw_bucket')}/orders_load_sample_001.csv"
    df = wr.s3.read_csv(filepath)
    df = df.drop_duplicates()
    df = df[df['product_id'].str.contains('GTX')]
    print(df.head())
    wr.s3.to_csv(df, f"s3://{read_config('qualified_bucket')}/orders_load_sample_001_processed.csv")

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