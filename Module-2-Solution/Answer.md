


##Answer 4
```
SELECT 
  SUM(row_count) as total_rows
FROM 
  `kestra-sandbox-484911.zoomcamp.__TABLES__`
WHERE 
  table_id LIKE 'green_tripdata_2020_%'
  AND table_id NOT LIKE '%ext'
```