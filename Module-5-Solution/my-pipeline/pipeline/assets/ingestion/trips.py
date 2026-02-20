"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: gcp-default

materialization:
  type: table
  strategy: delete+insert
  incremental_key: pickup_datetime
  time_granularity: month

columns:
  - name: pickup_datetime
    checks:
      - name: not_null
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime, timezone

def materialize():
    vars_json = os.getenv("BRUIN_VARS", "{}")
    variables = json.loads(vars_json)
    taxi_types = variables.get("taxi_types", ["yellow", "green"])
    
    start_date_str = os.getenv("BRUIN_START_DATE")
    dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    year, month = dt.year, f"{dt.month:02d}"

    all_dfs = []

    for taxi in taxi_types:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month}.parquet"
        try:
            df = pd.read_parquet(url)
            df.columns = [c.lower() for c in df.columns]

            # Define specific mappings and schemas based on taxi type
            if taxi == 'yellow':
                mapping = {
                    'vendorid': 'vendor_id',
                    'ratecodeid': 'rate_code_id',
                    'pulocationid': 'pickup_location_id',
                    'dolocationid': 'dropoff_location_id',
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                }
                # Columns to cast for Yellow
                schema = {
                    'vendor_id': 'Int64',
                    'rate_code_id': 'Int64',
                    'pickup_location_id': 'Int64',
                    'dropoff_location_id': 'Int64',
                    'pickup_datetime': 'datetime64[ns]',
                    'dropoff_datetime': 'datetime64[ns]',
                    'store_and_fwd_flag': 'string',
                    'passenger_count': 'Int64',
                    'trip_distance': 'float64',
                    'fare_amount': 'float64',
                    'extra': 'float64',
                    'mta_tax': 'float64',
                    'tip_amount': 'float64',
                    'tolls_amount': 'float64',
                    'improvement_surcharge': 'float64',
                    'total_amount': 'float64',
                    'payment_type': 'Int64'
                }
            elif taxi == 'green':
                mapping = {
                    'vendorid': 'vendor_id',
                    'ratecodeid': 'rate_code_id',
                    'pulocationid': 'pickup_location_id',
                    'dolocationid': 'dropoff_location_id',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                }
                # Columns to cast for Green (includes ehail_fee)
                schema = {
                    'vendor_id': 'Int64',
                    'rate_code_id': 'Int64',
                    'pickup_location_id': 'Int64',
                    'dropoff_location_id': 'Int64',
                    'pickup_datetime': 'datetime64[ns]',
                    'dropoff_datetime': 'datetime64[ns]',
                    'store_and_fwd_flag': 'string',
                    'passenger_count': 'Int64',
                    'trip_distance': 'float64',
                    'fare_amount': 'float64',
                    'extra': 'float64',
                    'mta_tax': 'float64',
                    'tip_amount': 'float64',
                    'tolls_amount': 'float64',
                    'ehail_fee': 'float64',
                    'improvement_surcharge': 'float64',
                    'total_amount': 'float64',
                    'payment_type': 'Int64'
                }
            else:
                continue

            # Rename columns based on mapping
            df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})

            # Apply casting based on schema
            for col, dtype in schema.items():
                if col in df.columns:
                    try:
                        df[col] = df[col].astype(dtype)
                    except Exception:
                        # Keep original column name/type if cast fails
                        pass
            
            # Select only the columns specified in the schema for this taxi type
            available_cols = [c for c in schema.keys() if c in df.columns]
            df = df[available_cols].copy()
            
            df['taxi_type'] = taxi
            df['extracted_at'] = datetime.now(timezone.utc)
            all_dfs.append(df)

        except Exception as e:
            print(f"No data for {taxi} in {year}-{month}: {e}")

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)



"""
@bruin
name: ingestion.trips
type: python
image: python:3.11
#connection: duckdb-default

materialization:
  type: table
  strategy: delete+insert
  incremental_key: pickup_datetime
  time_granularity: month
@bruin
""" 
