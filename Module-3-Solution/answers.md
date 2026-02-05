## Creating The External Table From Data in The Bucket

CREATE OR REPLACE EXTERNAL TABLE `data-warehouse-485815.hw.yellow_external_table`
OPTIONS(
  FORMAT='PARQUET',
  uris=['gs://dtc_zoomcamp_hw3_2026_cedan_murat_zeynepli/*.parquet']
)

## Creating The Materialized Table From External Table

CREATE OR REPLACE TABLE `data-warehouse-485815.hw.yellow_materialized_table`
AS
SELECT * 
FROM `data-warehouse-485815.hw.yellow_external_table`


## Answer 1 (20332093)

```
SELECT count (*)
FROM `data-warehouse-485815.hw.yellow_external_table`

```

## Answer 2 (0 - 155.12MB)

```
SELECT count (distinct PULocationID)
FROM `data-warehouse-485815.hw.yellow_external_table`;

SELECT count (distinct PULocationID)
FROM `data-warehouse-485815.hw.yellow_materialized_table`;

```

## Answer 3 (155.12 MB - 310.24 MB)

BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

```
SELECT 
PULocationID
FROM `data-warehouse-485815.hw.yellow_materialized_table`;

SELECT 
PULocationID,DOLocationID
FROM `data-warehouse-485815.hw.yellow_materialized_table`;
```

## Answer 4 (8333)

```
SELECT count(*)
FROM `data-warehouse-485815.hw.yellow_materialized_table`
WHERE fare_amount=0;

```

## Answer 5 (Partition by tpep_dropoff_datetime and Cluster on VendorID)

```
CREATE OR REPLACE TABLE `data-warehouse-485815.hw.yellow_optimized_table`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * 
FROM `data-warehouse-485815.hw.yellow_materialized_table`;
```

## Answer 6 (310.24 MB -  26.84 MB)

```
SELECT distinct VendorID
FROM `data-warehouse-485815.hw.yellow_materialized_table`
WHERE tpep_dropoff_datetime >='2024-03-01'
and tpep_dropoff_datetime <='2024-03-15';

SELECT distinct VendorID
FROM `data-warehouse-485815.hw.yellow_optimized_table`
WHERE tpep_dropoff_datetime >='2024-03-01'
and tpep_dropoff_datetime <='2024-03-15';
```

## Answer 7 

```
GCP Bucket
```

## Answer 8 

```
False
```


## Answer 9 (0 B)

```
SELECT count (*)
FROM `data-warehouse-485815.hw.yellow_materialized_table`
```