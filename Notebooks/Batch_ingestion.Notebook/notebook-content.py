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

import requests
import time
import calendar
from pyspark.sql import functions as F

BASE_URL = "https://aqs.epa.gov/data/api/dailyData/byState"

# param must be STRING (not list)
PARAMS = ["88101,44201,42602,42101,42401,81102"]

states = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", 
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", 
    "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", 
    "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", 
    "49", "50", "51", "52", "53", "54", "55", "56"
]

EMAIL = "agetien15@gmail.com"
KEY = "khakifrog24"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

    StructField("date_local", DateType(), True),

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

    StructField("date_of_last_change", DateType(), True),
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_with_retry(url, params, max_attempts=4, base_sleep=2.0, timeout=(10, 350)):
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
                sleep_s = base_sleep * (2 ** (attempt - 1))  # 2s, 4s, 8s, etc..
                print(f"Retry {attempt}/{max_attempts} failed for state={params.get('state')} "
                      f"bdate={params.get('bdate')} edate={params.get('edate')}: {e} "
                      f"Sleeping {sleep_s:.1f}s...")
                time.sleep(sleep_s)
            else:
                raise last_err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

years = [2026]
for year in years:
    for month in range(1,13):
        month_rows = []
        month_start_ts = time.time() 

        last_day = calendar.monthrange(year, month)[1]
        bdate = f"{year}{month:02d}01"
        edate = f"{year}{month:02d}{last_day}"

        print(f"\n--- Year {year} | Month {month:02d} ({bdate} → {edate}) ---")


        # Calls api for each state and pollutant, 50 states and 6 pollutants (2 strings)
        # 100+ API calls for a year?
        for state in states:
            
                # params is the actual API parameters
                params = {
                    "email": EMAIL,
                    "key": KEY,
                    "param": PARAMS,
                    "bdate": bdate,
                    "edate": edate,
                    "state": state
                }

                # try logic Adrian implemented
                try:
                    # could potentially change retry times to match api request limits? (max 10 per minute)
                    # maybe 4, 8, 16, 32 seconds
                    # 4 + 8 + 16 + 32 = 60 seconds
                    payload = get_with_retry(BASE_URL, params, max_attempts=4, base_sleep=4.0, timeout=(10, 350))
                    month_rows.extend(payload.get("Data", []))

                except Exception as e:
                    print(f"FAILED state={state} year={year}: {e}")

                time.sleep(0.25)

        # no rows in year
        if not month_rows:
            print(f"No rows in {month_rows}")
            continue

        # Convert to DataFrame
        df = spark.createDataFrame(month_rows)

        # writes to Files/ in Lakehouse
        df = df.withColumn("date_local", F.to_date("date_local"))
        df = (df.withColumn("year", F.year("date_local")).withColumn("month", F.month("date_local")))
        df.write.format('parquet').mode('append').partitionBy('year','month','state').save('Files/RAW/aqs_dailyByState')

        # removes year_rows and df for next month's data
        # del year_rows
        del df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
