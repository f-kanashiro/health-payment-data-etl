import os
import glob
import pandas as pd
import boto3
import fnmatch
from multiprocessing import Pool, cpu_count
from io import StringIO, TextIOWrapper

import config



def split_csv(csv):
    minio_client = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY
    )

    base_name = os.path.splitext(os.path.basename(csv))[0]

    raw_csv = minio_client.get_object(Bucket=config.MINIO_BUCKET_RAW_DATA, Key=csv)
    file_body = raw_csv["Body"]

    text_stream = TextIOWrapper(file_body, encoding="utf-8")

    csv_reader = pd.read_csv(
        text_stream,
        chunksize=config.ROWS_PER_CSV_SPLIT,
        quotechar='"',
        escapechar='\\',
        engine='python',
        dtype=str
    )

    for i, chunk in enumerate(csv_reader, start=1):
        out_key = f"{base_name}_part_{i}.csv"
        csv_buffer = StringIO()
        chunk.to_csv(csv_buffer, index=False)
        #csv_buffer.seek(0)

        minio_client.put_object(
            Bucket=config.MINIO_BUCKET_PROCESSED_DATA,
            Key=out_key,
            Body=csv_buffer.getvalue()
        )


def parallel_process_raw_csv():
    #csv_files = glob.glob(os.path.join(config.RAW_DATA_DIR, config.RAW_CSV_FILES_PATTERN))
    #if not csv_files:
        #return

    minio_client = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY
    )
    raw_files = minio_client.list_objects_v2(Bucket=config.MINIO_BUCKET_RAW_DATA)
    csv_files = [
        obj["Key"]
        for obj in raw_files.get("Contents", [])
        if fnmatch.fnmatch(obj["Key"], config.RAW_CSV_FILES_PATTERN)
    ]

    if not csv_files:
        return

    print("Test")
    with Pool(processes=cpu_count()) as pool:
        pool.map(split_csv, csv_files)

def run():
    parallel_process_raw_csv()