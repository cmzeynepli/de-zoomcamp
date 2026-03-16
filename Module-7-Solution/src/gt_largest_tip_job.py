from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_tips_sink(t_env):
    table_name = 'hourly_tips_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_tips DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source(t_env):
    table_name = "green_trips_source"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            tip_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def run_tips_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source = create_events_source(t_env)
        sink = create_tips_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {sink}
        SELECT
            window_start,
            window_end,
            SUM(tip_amount) AS total_tips
        FROM TABLE(
            TUMBLE(TABLE {source}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start, window_end;
        """).wait()
        
        print("Tips job completed successfully!")
        
    except Exception as e:
        print("Tips Job failed:", str(e))

if __name__ == '__main__':
    run_tips_job()