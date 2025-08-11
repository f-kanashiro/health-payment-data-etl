from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("health-payment-data-etl") \
    .getOrCreate()

df = spark.read.csv(
    "/home/fkanashiro/health-payment-data-etl/data/OP_DTL_GNRL_PGYR2019_P06302025_06162025.csv",
    header=True,
    inferSchema=True
)

df.show()