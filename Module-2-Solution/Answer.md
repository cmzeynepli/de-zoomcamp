## Answer 1 
134.5 MB = ~ 128.3 MiB

## Answer 2 
green_tripdata_2020-04.csv

## Answer 3 (24648499)
```
SELECT 
  SUM(row_count) as total_rows
FROM 
  `kestra-sandbox-484911.zoomcamp.__TABLES__`
WHERE 
  table_id LIKE 'yellow_tripdata_2020_%'
  AND table_id NOT LIKE '%ext'
```

## Answer 4 (1734051)
```
SELECT 
  SUM(row_count) as total_rows
FROM 
  `kestra-sandbox-484911.zoomcamp.__TABLES__`
WHERE 
  table_id LIKE 'green_tripdata_2020_%'
  AND table_id NOT LIKE '%ext'
```

## Answer 5 (1925152)
```
SELECT 
  SUM(row_count) as total_rows
FROM 
  `kestra-sandbox-484911.zoomcamp.__TABLES__`
WHERE 
  table_id LIKE 'green_tripdata_2020_03'
  AND table_id NOT LIKE '%ext'
```

## Answer 6 
Add a timezone property set to America/New_York in the Schedule trigger configuration
