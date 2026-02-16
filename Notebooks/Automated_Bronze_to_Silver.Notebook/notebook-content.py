# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "87c0302d-fecc-486b-9d67-943187cfd2a4",
# META       "default_lakehouse_name": "team2_LH",
# META       "default_lakehouse_workspace_id": "246d8a46-54fb-4891-b6c6-a0f96a6a126f",
# META       "known_lakehouses": [
# META         {
# META           "id": "87c0302d-fecc-486b-9d67-943187cfd2a4"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "b746263a-ea14-a08f-4d7a-7532c4448473",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# imports
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load in the data

BRONZE_PATH = "Files/BRONZE"

bronze_df = spark.read.parquet(BRONZE_PATH) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Delete later - just to see
display(bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Delete later or put in summary
bronze_df = spark.read.parquet("Files/BRONZE")
bronze_df.printSchema()
bronze_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Most recent EPA standard per pollutant — filters out duplicate rows
standard_filter = [
    "CO 8-hour 1971",
    "SO2 1-hour 2010",
    "NO2 1-hour 2010",
    "Ozone 8-hour 2015",
    "PM10 24-hour 2006",
    "PM25 24-hour 2012"
]

silver_measurement = (
    # Select only columns needed for silver_daily_measurement
    bronze_df
    # Select only columns needed for silver_daily_measurement
    .select(
        "state_code", "county_code", "site_number", "parameter_code", "poc",
        "date_local", "method_code",
        "arithmetic_mean", "first_max_value", "first_max_hour", "aqi",
        "observation_count", "observation_percent", "validity_indicator",
        "units_of_measure", "sample_duration", "event_type", "date_of_last_change",
        "pollutant_standard"
    )
    # Keep only one standard per pollutant to avoid row multiplication
    .filter(F.col("pollutant_standard").isin(standard_filter))
    # No longer needed after filtering
    .drop("pollutant_standard")
    # Remove duplicate measurements on composite key
    .dropDuplicates(["state_code", "county_code", "site_number", "parameter_code", "poc", "date_local"])
    # Remove invalid/incomplete rows
    .filter(
        (F.col("validity_indicator") == "Y") &
        (F.col("arithmetic_mean").isNotNull()) &
        (F.col("aqi").isNotNull())
    )
)

silver_measurement.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create silver_site table
silver_site = (
    bronze_df.select(
        "state_code",
        "county_code",
        "site_number",
        "latitude",
        "longitude",
        "datum",
        "local_site_name",
        "site_address",
        "city",
        "cbsa_code"
    )
    .dropDuplicates(["state_code", "county_code", "site_number"])
)

# Create silver_admin_area table
silver_admin_area = (
    bronze_df.select(
        "state_code",
        "county_code",
        F.col("state").alias("state_name"),
        F.col("county").alias("county_name")
    )
    .dropDuplicates(["state_code", "county_code"])
)

# silver_parameter
silver_parameter = (
    bronze_df.select(
        "parameter_code",
        F.col("parameter").alias("parameter_name")
    )
    .dropDuplicates(["parameter_code"])
)

# silver_method
silver_method = (
    bronze_df.select(
        "method_code",
        F.col("method").alias("method_name")
    )
    .dropDuplicates(["method_code"])
)

# silver_cbsa
silver_cbsa = (
    bronze_df.select(
        "cbsa_code",
        F.col("cbsa").alias("cbsa_name")
    )
    .dropDuplicates(["cbsa_code"])
    .filter(F.col("cbsa_code").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Saving to delta
silver_measurement.write.format("delta").mode("overwrite").saveAsTable("auto_silver_measurement")
silver_site.write.format("delta").mode("overwrite").saveAsTable("auto_silver_site")
silver_measurement.write.format("delta").mode("overwrite").saveAsTable("auto_silver_daily_measurement")
silver_admin_area.write.format("delta").mode("overwrite").saveAsTable("auto_silver_admin_area")
silver_parameter.write.format("delta").mode("overwrite").saveAsTable("auto_silver_parameter")
silver_method.write.format("delta").mode("overwrite").saveAsTable("auto_silver_method")
silver_cbsa.write.format("delta").mode("overwrite").saveAsTable("auto_silver_cbsa")

# WHEN ACTUALLY IN PRODUCTION, SWITCH TO IF_END and DELTA MERGE

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = [
    "auto_silver_daily_measurement",
    "auto_silver_site", 
    "auto_silver_admin_area",
    "auto_silver_parameter",
    "auto_silver_method",
    "auto_silver_cbsa"
]

for t in tables:
    count = spark.table(t).count()
    print(f"{t}: {count} rows")

# THINGS YOU HAVE TO DO IN PRODUCTION:
#
# MERGE instead of overwrite when writing to Delta
# Read only new data, not all of Bronze (pass bdate/edate as parameters, filter on date_local)
# Dimension tables need MERGE too
# Paramaterized Cells 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
