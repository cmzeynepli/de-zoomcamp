# Solution of Module 1 

## Answer 1. Understanding Docker images (25.3)
- Firstly it should be writen ` docker run --rm -it python:3.13 bash` command into terminal for starting and opening python:3.13 container's bash termial.
- After opening the container's terminal, the pip version can be viewed using the `pip --version` command.

## Answer 2. Understanding Docker networking and docker-compose (db:5433)
- The hostname is located below the services line (which starts the containers information).
- The port number is the first port number shown under the ports label.


## Answer 3. Counting short trips (8007)
```
SELECT COUNT(*) AS trip_count
FROM yellow_taxi_data
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

## Answer 4. Longest trip for each day (2025-11-14)
```
SELECT
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS max_trip_distance
FROM yellow_taxi_data
WHERE trip_distance < 100
GROUP BY pickup_day
ORDER BY max_trip_distance DESC
LIMIT 1;
```

## Answer 5. Biggest pickup zone (East Harlem North)
```
SELECT
    tzl."Zone",
    SUM(ytd.total_amount) AS total_revenue
FROM yellow_taxi_data AS ytd
RIGHT JOIN taxi_zone_lookup AS tzl
ON ytd."PULocationID" = tzl."LocationID"
WHERE lpep_pickup_datetime >= '2025-11-18'
  AND lpep_pickup_datetime < '2025-11-19'
GROUP BY tzl."Zone"
ORDER BY total_revenue DESC
LIMIT 1;
```

## Answer 6. Largest tip (Yorkville West)
```
SELECT
    tzl_do."Zone" AS dropoff_zone,
    ytd."tip_amount"
FROM yellow_taxi_data AS ytd
JOIN taxi_zone_lookup AS tzl_pu
  ON ytd."PULocationID" = tzl_pu."LocationID"
JOIN taxi_zone_lookup AS tzl_do
  ON ytd."DOLocationID" = tzl_do."LocationID"
WHERE tzl_pu."Zone" = 'East Harlem North'
  AND ytd.lpep_pickup_datetime >= '2025-11-01'
  AND ytd.lpep_pickup_datetime < '2025-12-01'
ORDER BY ytd."tip_amount" DESC
LIMIT 1;
```