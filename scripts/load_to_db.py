from google.cloud import storage
from pathlib import Path
import glob
import config

def run():
    print("Running!")
    client = storage.Client()
    bucket = client.bucket("health_payment_data_bucket")

    for file in Path(config.READY_DATA_DIR).rglob("*.parquet"):
        print("File find")
        blob = bucket.blob(file.name)
        blob.upload_from_filename(str(file))