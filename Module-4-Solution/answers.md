## Answer 1 

```It is only build int_trips_unioned.sql```

## Answer 2

``` dbt will fail the test, returning a non-zero exit code ```

## Answer 3 (12998)

```
SELECT count(*)  FROM {{ref('fct_monthly_zone_revenue')}} 
```

## Answer 4 (East Harlem North)

```
SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM {{ref('fct_monthly_zone_revenue')}} 
WHERE service_type='Green'
  AND revenue_month >= '2020-01-01' 
  AND revenue_month <= '2020-12-31'
GROUP BY 1
ORDER BY total_revenue DESC
LIMIT 1;
```

## Answer 5 (384624)

```
SELECT sum(total_monthly_trips)
FROM {{ref('fct_monthly_zone_revenue')}} 
WHERE service_type='Green'
    AND revenue_month BETWEEN '2019-10-01' AND '2019-10-31';
```

## Answer 6 (43244693)

- added into sources.yml

```
- name: fhv_tripdata
        description: > 
          Raw For-Hire Vehicle (FHV) trip records for 2019. 
          Note: This dataset contains only high-level trip info.
        columns:
          - name: dispatching_base_num
          - name: pickup_datetime
          - name: dropoff_datetime
          - name: pulocationid
          - name: dolocationid
          - name: sr_flag
          - name: affiliated_base_number
```

- added into schema.yml

```
- name: stg_fhv_tripdata
    description: >
      Staging model for FHV trip data. Filters records where dispatching_base_num is null 
      and standardizes column names to match the project convention.
    columns:
      - name: dispatching_base_num
        data_tests:
          - not_null
      - name: pickup_datetime
        data_tests:
          - not_null
      - name: pickup_location_id
        description: TLC Taxi Zone ID for pickup
      - name: dropoff_location_id
        description: TLC Taxi Zone ID for dropoff
```


- stg_fhv_tripdata.sql file:

```
{{ config(materialized='view') }}

with tripdata as 
(
  select *
  from {{ source('raw', 'fhv_tripdata') }}
  where dispatching_base_num is not null
)

select
    -- identifiers
    dispatching_base_num,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- location IDs
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
   

from tripdata

```

- Counting :

```
SELECT count(*) 
FROM {{ ref('stg_fhv_tripdata') }};
```
