## Answer 1
```
2009-06-01 to 2009-07-01
```

## Answer 2 (26.66%)
```
SELECT 
    AVG(CASE WHEN payment_type = 'Credit' THEN 1.0 ELSE 0.0 END) AS proportion
FROM "nyc_taxi_trips";
```

## Answer 3 ($6,063.41)
```
SELECT
  SUM (tip_amt)
FROM "nyc_taxi_trips"
```