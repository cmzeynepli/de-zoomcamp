/* @bruin
name: reports.trips_report
type: bq.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

hooks:
  pre:
    - query: |
        CREATE TABLE IF NOT EXISTS `reports.trips_report` AS
        SELECT 
            CAST(pickup_datetime AS DATE) AS trip_date,
            taxi_type,
            payment_type,
            MAX(payment_type_description) AS payment_type_description,
            count(trip_id) AS total_trips,
            SUM(passenger_count) AS total_passengers,
            SUM(trip_distance) AS total_distance,
            SUM(fare_amount) AS total_fare_amount,
            sum(total_amount) AS total_revenue,
            AVG(trip_distance) AS avg_trip_distance
        FROM `staging.trips`
        WHERE 1 = 0
        GROUP BY trip_date, taxi_type, payment_type
        LIMIT 0

columns:
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    primary_key: true
  - name: payment_type_description
    type: string
    description: Human-readable payment method
    primary_key: true
  - name: trip_date
    type: date
    description: Date of the trips
    primary_key: true
  - name: total_trips
    type: bigint
    description: Total number of trips in this window
    checks:
      - name: non_negative
  - name: total_revenue
    type: numeric
    description: Total amount collected including tips and fees
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: numeric
    description: Average distance traveled per trip
@bruin */

SELECT 
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type,
    MAX(payment_type_description) AS payment_type_description,
    count(trip_id) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance,
    SUM(fare_amount) AS total_fare_amount,
    sum(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance

FROM `staging.trips`
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 
    trip_date,
    taxi_type,
    payment_type