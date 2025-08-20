import os
import glob
import pandas as pd
from multiprocessing import Pool, cpu_count

import config

def split_csv(csv):
    base_name = os.path.splitext(os.path.basename(csv))[0]
    #os.makedirs(config.PROCESSED_DATA_DIR, exist_ok=True)

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
        out_path = os.path.join(config.PROCESSED_DATA_DIR, f"{base_name}_part_{i}.csv")
        chunk.to_csv(out_path, index=False)
        print(f"[OK] Written {out_path} with {len(chunk)} rows")


def parallel_process_raw_csv():
    csv_files = glob.glob(os.path.join(config.RAW_DATA_DIR, config.RAW_CSV_FILES_PATTERN))
    if not csv_files:
        return

    with Pool(processes=cpu_count()) as pool:
        pool.map(split_csv, csv_files)

def run():
    parallel_process_raw_csv()