from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when, concat, row_number)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
import config

class HealthDataProcessing:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("health-payment-data-etl")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .getOrCreate()
        )

        data_schema = StructType([
            StructField("Change_Type", StringType(), False),
            StructField("Covered_Recipient_Type", StringType(), False),
            StructField("Teaching_Hospital_CCN", IntegerType(), True),
            StructField("Teaching_Hospital_ID", IntegerType(), True),
            StructField("Teaching_Hospital_Name", StringType(), True),
            StructField("Covered_Recipient_Profile_ID", IntegerType(), True),
            StructField("Covered_Recipient_NPI", IntegerType(), True),
            StructField("Covered_Recipient_First_Name", StringType(), True),
            StructField("Covered_Recipient_Middle_Name", StringType(), True),
            StructField("Covered_Recipient_Last_Name", StringType(), True),
            StructField("Covered_Recipient_Name_Suffix", StringType(), True),
            StructField("Recipient_Primary_Business_Street_Address_Line1", StringType(), False),
            StructField("Recipient_Primary_Business_Street_Address_Line2", StringType(), True),
            StructField("Recipient_City", StringType(), False),
            StructField("Recipient_State", StringType(), True),
            StructField("Recipient_Zip_Code", StringType(), True),
            StructField("Recipient_Country", StringType(), False),
            StructField("Recipient_Province", StringType(), True),
            StructField("Recipient_Postal_Code", StringType(), True),
            StructField("Covered_Recipient_Primary_Type_1", StringType(), True),
            StructField("Covered_Recipient_Primary_Type_2", StringType(), True),
            StructField("Covered_Recipient_Primary_Type_3", StringType(), True),
            StructField("Covered_Recipient_Primary_Type_4", StringType(), True),
            StructField("Covered_Recipient_Primary_Type_5", StringType(), True),
            StructField("Covered_Recipient_Primary_Type_6", StringType(), True),
            StructField("Covered_Recipient_Specialty_1", StringType(), True),
            StructField("Covered_Recipient_Specialty_2", StringType(), True),
            StructField("Covered_Recipient_Specialty_3", StringType(), True),
            StructField("Covered_Recipient_Specialty_4", StringType(), True),
            StructField("Covered_Recipient_Specialty_5", StringType(), True),
            StructField("Covered_Recipient_Specialty_6", StringType(), True),
            StructField("Covered_Recipient_License_State_code1", StringType(), True),
            StructField("Covered_Recipient_License_State_code2", StringType(), True),
            StructField("Covered_Recipient_License_State_code3", StringType(), True),
            StructField("Covered_Recipient_License_State_code4", StringType(), True),
            StructField("Covered_Recipient_License_State_code5", StringType(), True),
            StructField("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", StringType(), False),
            StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID", IntegerType(), False),
            StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", StringType(), False),
            StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", StringType(), True),
            StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country", StringType(), False),
            StructField("Total_Amount_of_Payment_USDollars", DecimalType(), False),
            StructField("Date_of_Payment", DateType(), False),
            StructField("Number_of_Payments_Included_in_Total_Amount", IntegerType(), False),
            StructField("Form_of_Payment_or_Transfer_of_Value", StringType(), False),
            StructField("Nature_of_Payment_or_Transfer_of_Value", StringType(), False),
            StructField("City_of_Travel", StringType(), True),
            StructField("State_of_Travel", StringType(), True),
            StructField("Country_of_Travel", StringType(), True),
            StructField("Physician_Ownership_Indicator", StringType(), True),
            StructField("Third_Party_Payment_Recipient_Indicator", StringType(), False),
            StructField("Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value", StringType(), True),
            StructField("Charity_Indicator", StringType(), True),
            StructField("Third_Party_Equals_Covered_Recipient_Indicator", StringType(), True),
            StructField("Contextual_Information", StringType(), True),
            StructField("Delay_in_Publication_Indicator", StringType(), False),
            StructField("Record_ID", IntegerType(), False),
            StructField("Dispute_Status_for_Publication", StringType(), False),
            StructField("Related_Product_Indicator", StringType(), False),
            StructField("Covered_or_Noncovered_Indicator_1", StringType(), True),
            StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1", StringType(), False),
            StructField("Product_Category_or_Therapeutic_Area_1", StringType(), True),
            StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1", StringType(), True),
            StructField("Associated_Drug_or_Biological_NDC_1", StringType(), True),
            StructField("Associated_Device_or_Medical_Supply_PDI_1", StringType(), True),
            StructField("Covered_or_Noncovered_Indicator_2", StringType(), True),
            StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2", StringType(), True),
            StructField("Product_Category_or_Therapeutic_Area_2", StringType(), True),
            StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2", StringType(), True),
            StructField("Associated_Drug_or_Biological_NDC_2", StringType(), True),
            StructField("Associated_Device_or_Medical_Supply_PDI_2", StringType(), True),
            StructField("Covered_or_Noncovered_Indicator_3", StringType(), True),
            StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3", StringType(), True),
            StructField("Product_Category_or_Therapeutic_Area_3", StringType(), True),
            StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3", StringType(), True),
            StructField("Associated_Drug_or_Biological_NDC_3", StringType(), True),
            StructField("Associated_Device_or_Medical_Supply_PDI_3", StringType(), True),
            StructField("Covered_or_Noncovered_Indicator_4", StringType(), True),
            StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4", StringType(), True),
            StructField("Product_Category_or_Therapeutic_Area_4", StringType(), True),
            StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4", StringType(), True),
            StructField("Associated_Drug_or_Biological_NDC_4", StringType(), True),
            StructField("Associated_Device_or_Medical_Supply_PDI_4", StringType(), True),
            StructField("Covered_or_Noncovered_Indicator_5", StringType(), True),
            StructField("Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5", StringType(), True),
            StructField("Product_Category_or_Therapeutic_Area_5", StringType(), True),
            StructField("Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5", StringType(), True),
            StructField("Associated_Drug_or_Biological_NDC_5", StringType(), True),
            StructField("Associated_Device_or_Medical_Supply_PDI_5", StringType(), True),
            StructField("Program_Year", IntegerType(), False),
            StructField("Payment_Publication_Date", DateType(), False)
        ])

        self.df_raw_health_payment_data = self.spark.read.csv(
            config.PROCESSED_DATA_DIR + config.RAW_CSV_FILES_PATTERN,
            header=True,
            schema=data_schema,
            quote='"',
            escape='"',
            multiLine=True
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

        df_manufacturer_dimension.write.mode("overwrite").parquet(config.READY_DATA_DIR + "manufacturer/")

    def process_recipient_dimension(self):
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

        df_recipient_dimension.write.mode("overwrite").parquet(config.READY_DATA_DIR + "recipient/")

    def process_hospital_dimension(self):
        teaching_hospital_id_window = Window.partitionBy("Teaching_Hospital_ID").orderBy("Teaching_Hospital_ID")  

        df_teaching_hospital_dimension = (self.df_raw_health_payment_data.select(
            "Teaching_Hospital_ID",
            "Teaching_Hospital_CCN",
            "Teaching_Hospital_Name"
        )
        .withColumn("row_num", row_number().over(teaching_hospital_id_window))
        .filter(col("row_num") == 1)
        .drop("row_num"))

        df_teaching_hospital_dimension = df_teaching_hospital_dimension.withColumn(
            "teaching_hospital_natural_key", col("Teaching_Hospital_ID")
        )

        df_teaching_hospital_dimension.write.mode("overwrite").parquet(config.READY_DATA_DIR + "teaching_hospital/")

def run():
    HDP = HealthDataProcessing()    
    HDP.process_manufacturer_dimension()
    HDP.process_hospital_dimension()
    HDP.process_recipient_dimension()