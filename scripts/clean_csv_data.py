import csv
import glob
import os
import config
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

def clean_csv_file(input_path: Path, output_path: Path):
    with input_path.open("r", encoding="utf-8", newline="") as fr, \
         output_path.open("w", encoding="utf-8", newline="") as fw:
        reader = csv.reader(fr, quotechar='"', delimiter=',', escapechar='\\')
        writer = csv.writer(fw, quotechar='"', delimiter=',', escapechar='\\', quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            # Ensure row has enough columns (skip malformed lines gracefully)
            if len(row) > config.PROCESS_POOL_MAX_WORKERS:
                field = row[config.PROCESS_POOL_MAX_WORKERS]
                row[config.PROCESS_POOL_MAX_WORKERS] = (
                    field.replace("\r\n", "\\n")
                         .replace("\n", "\\n")
                         .replace("\r", "\\r")
                )
            writer.writerow(row)

def clean_multiple_csvs_parallel():
    Path(config.PROCESSED_DATA_DIR).mkdir(parents=True, exist_ok=True)

    csv_files = list(Path(config.RAW_DATA_DIR).glob(config.RAW_CSV_FILES_PATTERN))
    tasks = []

    with ProcessPoolExecutor(max_workers=config.PROCESS_POOL_MAX_WORKERS) as executor:
        for csv_path in csv_files:
            output_path = Path(config.PROCESSED_DATA_DIR) / f"{csv_path.stem}_clean.csv"
            tasks.append(
                executor.submit(clean_csv_file, csv_path, output_path)
            )

        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                print(f"Error: {e}")

def run():
    clean_multiple_csvs_parallel()