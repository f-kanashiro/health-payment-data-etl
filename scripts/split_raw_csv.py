import os
import glob
import pandas as pd
import boto3
from multiprocessing import Pool, cpu_count
from io import StringIO

import config

minio_client = boto3.client(
    "s3",
    endpoint_url=config.MINIO_ENDPOINT,
    aws_access_key_id=config.MINIO_ACCESS_KEY,
    aws_secret_access_key=config.MINIO_SECRET_KEY
)

def split_csv(csv):
    base_name = os.path.splitext(os.path.basename(csv))[0]

    csv_reader = pd.read_csv(
        csv,
        chunksize=config.ROWS_PER_CSV_SPLIT,
        quotechar='"',
        escapechar='\\',
        engine='python',
        encoding='utf-8',
        dtype=str
    )

    for i, chunk in enumerate(csv_reader, start=1):
        out_key = f"{base_name}_part_{i}.csv"
        csv_buffer = StringIO()
        chunk.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        minio_client.put_object(
            Bucket=config.MINIO_BUCKET_PROCESSED_DATA,
            Key=out_key,
            Body=csv_buffer.getvalue()
        )


def parallel_process_raw_csv():
    csv_files = glob.glob(os.path.join(config.RAW_DATA_DIR, config.RAW_CSV_FILES_PATTERN))
    if not csv_files:
        return

    with Pool(processes=cpu_count()) as pool:
        pool.map(split_csv, csv_files)

def run():
    parallel_process_raw_csv()