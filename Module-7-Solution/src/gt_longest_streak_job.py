import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def run_longest_session_job():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_ddl = """
    CREATE TABLE green_trips_source (
        lpep_pickup_datetime STRING,
        PULocationID INT,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """
    t_env.execute_sql(source_ddl)

    sink_ddl = """
    CREATE TABLE longest_session_result (
        PULocationID INT,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        trip_count BIGINT,
        PRIMARY KEY (PULocationID) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'longest_session_result',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
    """
    t_env.execute_sql(sink_ddl)

    final_query = """
    INSERT INTO longest_session_result
    SELECT
        PULocationID,
        window_start,
        window_end,
        trip_count
    FROM (
        SELECT
            PULocationID,
            window_start,
            window_end,
            COUNT(*) AS trip_count,
            -- Rank all sessions across all locations by trip count
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
        FROM TABLE(
            SESSION(
                TABLE green_trips_source
                PARTITION BY PULocationID,  
                DESCRIPTOR(event_timestamp),
                INTERVAL '5' MINUTES
            )
        )
        GROUP BY PULocationID, window_start, window_end
    )
    WHERE rn = 1  -- Get only the longest session
    """

    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(final_query)

    print("Submitting Flink job → http://localhost:8081")

    statement_set.execute()


if __name__ == "__main__":
    run_longest_session_job()