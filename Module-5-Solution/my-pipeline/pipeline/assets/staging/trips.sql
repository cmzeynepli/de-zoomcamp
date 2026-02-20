/* @bruin
name: staging.trips
type: bq.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

hooks:
  pre:
    - query: |
        CREATE TABLE IF NOT EXISTS `staging.trips` AS
        WITH raw_data AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY vendor_id, pickup_datetime, pickup_location_id ORDER BY extracted_at DESC) as rn
          FROM `ingestion.trips`
          WHERE 1 = 0
        ),
        cleaned_data AS (
          SELECT
            TO_HEX(MD5(CONCAT(CAST(vendor_id AS STRING), CAST(pickup_datetime AS STRING), CAST(pickup_location_id AS STRING)))) as trip_id,
            vendor_id, pickup_datetime, dropoff_datetime,
            TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) as duration_minutes,
            passenger_count, trip_distance, rate_code_id, store_and_fwd_flag,
            pickup_location_id, dropoff_location_id, payment_type,
            fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
            CAST(total_amount AS NUMERIC) as total_amount, taxi_type
          FROM raw_data
          WHERE rn = 1
        )
        SELECT t.*, p.payment_type_name AS payment_type_description
        FROM cleaned_data t
        LEFT JOIN `ingestion.payment_lookup` p ON t.payment_type = p.payment_type_id
        LIMIT 0

columns:
  - name: trip_id
    type: string
    primary_key: true
    checks:
      - name: not_null
  - name: total_amount
    type: numeric
    checks:
      - name: non_negative


custom_checks:
  - name: check_distinct_trips
    description: Ensure no duplicate trips exist
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT trip_id)
      FROM (
          SELECT TO_HEX(MD5(CONCAT(CAST(vendor_id AS STRING), CAST(pickup_datetime AS STRING), CAST(pickup_location_id AS STRING)))) as trip_id
          FROM (
              SELECT vendor_id, pickup_datetime, pickup_location_id,
                     ROW_NUMBER() OVER (PARTITION BY vendor_id, pickup_datetime, pickup_location_id ORDER BY extracted_at DESC) as rn
              FROM `ingestion.trips`
              WHERE pickup_datetime >= '{{ start_datetime }}'
                AND pickup_datetime < '{{ end_datetime }}'
          )
          WHERE rn = 1
      )
    value: 0
@bruin */

WITH raw_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY vendor_id, pickup_datetime, pickup_location_id 
            ORDER BY extracted_at DESC
        ) as rn
    FROM `ingestion.trips`
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime < '{{ end_datetime }}'
),

cleaned_data AS (
    SELECT
        
        TO_HEX(MD5(CONCAT(CAST(vendor_id AS STRING), CAST(pickup_datetime AS STRING), CAST(pickup_location_id AS STRING)))) as trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) as duration_minutes,
        passenger_count,
        trip_distance,
        rate_code_id,
        store_and_fwd_flag,
        pickup_location_id,
        dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        CAST(total_amount AS NUMERIC) as total_amount,
        taxi_type
    FROM raw_data
    WHERE rn = 1 
      AND vendor_id IS NOT NULL
      AND total_amount >= 0
)

SELECT 
    t.*,
    p.payment_type_name AS payment_type_description 
FROM cleaned_data t
LEFT JOIN `ingestion.payment_lookup` p 
    ON t.payment_type = p.payment_type_id


