from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when, concat)

spark = SparkSession.builder \
    .appName("health-payment-data-etl") \
    .getOrCreate()

df_raw_health_payment_data = spark.read.csv(
    "/home/fkanashiro/health-payment-data-etl/data/raw/OP_DTL_GNRL_PGYR2019_P06302025_06162025.csv",
    header=True,
    inferSchema=True
)

#df_raw_health_payment_data.printSchema()

df_dimension_recipient = df_raw_health_payment_data.select(
    "Covered_Recipient_Profile_ID",
    "Covered_Recipient_NPI",
    "Covered_Recipient_First_Name",
    "Covered_Recipient_Middle_Name",
    "Covered_Recipient_Last_Name",
    "Covered_Recipient_Name_Suffix",
    "Recipient_Primary_Business_Street_Address_Line1",
    "Recipient_Primary_Business_Street_Address_Line2",
    "Recipient_City",
    "Recipient_State",
    "Recipient_Zip_Code",
    "Recipient_Country",
    "Recipient_Province",
    "Recipient_Postal_Code",
    "Covered_Recipient_Primary_Type_1",
    "Covered_Recipient_Specialty_1",
    "Covered_Recipient_License_State_code1"
).dropDuplicates()

df_dimension_recipient = df_dimension_recipient.withColumn(
    "recipient_natural_key", col("Covered_Recipient_Profile_ID")
)

#df_dimension_recipient.show()

df_dimension_recipient.write.mode("overwrite").parquet("/home/fkanashiro/health-payment-data-etl/data/processed")

df_dimension_recipient_parquet = spark.read.parquet("/home/fkanashiro/health-payment-data-etl/data/processed")

df_dimension_recipient_parquet.show()