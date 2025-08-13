from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when, concat, row_number)
from pyspark.sql.window import Window

class HealthDataProcessing:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("health-payment-data-etl")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .getOrCreate()
        )

        self.df_raw_health_payment_data = self.spark.read.csv(
            "/home/fkanashiro/health-payment-data-etl/data/raw/OP_DTL_GNRL_*.csv",
            header=True,
            inferSchema=True
        )

    def process_manufacturer_dimension(self):
        manufacturer_id_window = Window.partitionBy("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID").orderBy("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID")

        df_manufacturer_dimension = (self.df_raw_health_payment_data.select(
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",
            "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country"
        )
        .withColumn("row_num", row_number().over(manufacturer_id_window))
        .filter(col("row_num") == 1)
        .drop("row_num"))

        df_manufacturer_dimension = df_manufacturer_dimension.withColumn(
            "manufacturer_natural_key", col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID")
        )

        df_manufacturer_dimension.write.mode("overwrite").parquet("/home/fkanashiro/health-payment-data-etl/data/processed/manufacturer")

    def process_recipient_dimension(self):
        #df_raw_health_payment_data = self.spark.read.csv(
        #    "/home/fkanashiro/health-payment-data-etl/data/raw/OP_DTL_GNRL_*.csv",
        #    header=True,
        #    inferSchema=True
        #)

        recipient_id_window = Window.partitionBy("Covered_Recipient_Profile_ID").orderBy("Covered_Recipient_Profile_ID")

        df_recipient_dimension = (self.df_raw_health_payment_data.select(
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
        )
        .withColumn("row_num", row_number().over(recipient_id_window))
        .filter(col("row_num") == 1)
        .drop("row_num"))

        df_recipient_dimension = df_recipient_dimension.withColumn(
            "recipient_natural_key", col("Covered_Recipient_Profile_ID")
        )

        df_recipient_dimension.write.mode("overwrite").parquet("/home/fkanashiro/health-payment-data-etl/data/processed/recipient")


HDP = HealthDataProcessing()
#HDP.process_recipient_dimension()
HDP.process_manufacturer_dimension()