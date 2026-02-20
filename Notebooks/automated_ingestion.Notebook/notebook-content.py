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

# PARAMETERS CELL ********************

# Imports and array definitons 

import requests
import time
import calendar
from pyspark.sql import functions as F

# Config
API_EMAIL = "alfred.ramos3519@outlook.com"
API_KEY = "bayhawk89"
bdate = "20251030"
edate = "20251031"
print("bdate:", bdate)
print("edate:", edate)
BASE_URL = "https://aqs.epa.gov/data/api/dailyData/byState"
OUTPUT_PATH = "Files/BRONZE"

state_array = [ "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", 
                "13", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", 
                "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", 
                "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", 
                "49", "50", "51", "53", "54", "55", "56"]
               
param_array = ['42101,42401,42602','44201,81102,88101']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# NEW - FIXED SCHEMA
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

aqs_daily_schema = StructType([
    StructField("state_code", StringType(), True),
    StructField("county_code", StringType(), True),
    StructField("site_number", StringType(), True),
    StructField("parameter_code", StringType(), True),
    StructField("poc", IntegerType(), True),

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("datum", StringType(), True),

    StructField("parameter", StringType(), True),
    StructField("sample_duration", StringType(), True),
    StructField("pollutant_standard", StringType(), True),

    StructField("date_local", StringType(), True),

    StructField("units_of_measure", StringType(), True),
    StructField("event_type", StringType(), True),

    StructField("observation_count", IntegerType(), True),
    StructField("observation_percent", DoubleType(), True),
    StructField("validity_indicator", StringType(), True),

    StructField("arithmetic_mean", DoubleType(), True),
    StructField("first_max_value", DoubleType(), True),
    StructField("first_max_hour", IntegerType(), True),
    StructField("aqi", IntegerType(), True),

    StructField("method_code", StringType(), True),
    StructField("method", StringType(), True),

    StructField("local_site_name", StringType(), True),
    StructField("site_address", StringType(), True),
    StructField("county", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),

    StructField("cbsa_code", StringType(), True),
    StructField("cbsa", StringType(), True),

    StructField("date_of_last_change", StringType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_with_retry(url, params, max_attempts=4, base_sleep=2.0, timeout=(10, 120)):
    """
    Retries transient failures with exponential backoff.
    base_sleep=2s => sleeps 2, 4, 8... seconds between attempts.
    """
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                sleep_s = base_sleep * (2 ** (attempt - 1))
                print(f"Retry {attempt}/{max_attempts} failed for state={params.get('state')} "
                      f"bdate={params.get('bdate')} edate={params.get('edate')}: {e}. "
                      f"Sleeping {sleep_s:.1f}s...")
                time.sleep(sleep_s)
            else:
                raise last_err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Currently we have the partition by state but will update later

# CELL ********************

from datetime import datetime

# For log
start_time = datetime.now()
call_count = 0
total_rows = 0
total_calls = len(param_array) * len(state_array)

# Sets are for determining how many states have data total in the summary
states_param1 = set()
states_param2 = set()
param_index = 0 # Keep track of where we're at

# Primary for loop to grab 3 pollutants at a time
for param in param_array:
    param_index += 1
    # Secondary loop to grab all states for the API call
    for state in state_array:
        
        # Log
        call_count += 1
        
        # Dictionary used to the build the API Call
        params = {
            "email": API_EMAIL,
            "key": API_KEY,
            "param": param,
            "bdate": bdate,
            "edate": edate,
            "state": state
        }

        # Build API call, Chris: Put in Try-Except block to stop errors
        try:
            data = get_with_retry(BASE_URL, params)
        except Exception as e:
            print(f"  [{call_count}/{total_calls}] [FAIL] State {state}: {e}")
            continue
            # Have it pick up where it failed - continue? break?

        # Chris: Skip if no data has been returned - someone on teams mentioned no data for certain states? 
        if "Data" not in data or not data["Data"]:
            print(f"  [{call_count}/{total_calls}] State {state}: no data, skipping")
            continue
        else:
            try:
                df = spark.createDataFrame(data['Data'], schema = aqs_daily_schema) # Chris: Felt this made more sense outside of try block
            except Exception as e:
                print(f"  [{call_count}/{total_calls}] State {state}: schema error, skipping — {e}")
                continue
            
            # Write out the data frame after each state pull to ensure we continue to save the data
            
            df = df.withColumn("date_local", F.to_date("date_local"))
            df = (df.withColumn("year", F.year("date_local")).withColumn("month", F.month("date_local")))
            df.repartition('state').write.format('parquet').mode('overwrite').option("partitionOverwriteMode", "dynamic") \
            .partitionBy('year', 'month', 'date_local', 'state').save(OUTPUT_PATH)
            
            print(f"{call_count}/{total_calls}; On State {state}: {len(data['Data'])} rows written.") # Logging
            total_rows += len(data['Data'])

            if param_index == 1:
                states_param1.add(state)
            else:
                states_param2.add(state)

        time.sleep(1) # Chris: Might be unnecessary, but Delay between API calls to avoid rate limiting
        # Put after so it doesnt check for empty first

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Summary Cell
end_time = datetime.now()

print(f"INGESTION COMPLETE")

print(f"  Date range:      {bdate} to {edate}")
print(f"  Duration:        {end_time - start_time}")
print(f"  Total calls:     {call_count}")
print(f"  Total rows:      {total_rows:,}")

states_both = states_param1 | states_param2  # intersection — states with BOTH groups
print(f"  States with all pollutants: {len(states_both)}/51")
print(f"  States with partial data:   {len(states_param1 | states_param2) - len(states_both)}")

if len(states_both) < 45:
    raise Exception(f"Only {len(states_both)}/51 states have complete data. Date range may be too recent.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
