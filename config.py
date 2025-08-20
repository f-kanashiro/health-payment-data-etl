#Directories
RAW_DATA_DIR = "/home/fkanashiro/health-payment-data-etl/data/raw/"
PROCESSED_DATA_DIR = "/home/fkanashiro/health-payment-data-etl/data/processed/"
READY_DATA_DIR = "/home/fkanashiro/health-payment-data-etl/data/ready/"
#Data Files
RAW_CSV_FILES_PATTERN = "OP_DTL_GNRL_PGYR*.csv"
#Data Processing Params
ROWS_PER_CSV_SPLIT = 250_000
IDX_CONTEXTUAL_INFORMATION_COLUMN = 54
#Spark Configuration
PROCESS_POOL_MAX_WORKERS = 4