import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pyarrow.parquet as pq
import click
import requests
import os
from datetime import date

dtype_csv_file={
    "LocationID":"int64",
    "Borough":"string",
    "Zone":"string",
    "service_zone":"string"    
}

parquet_url_prefix="https://d37ci6vzurychx.cloudfront.net/trip-data/"
csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

def download_file(url, output_path):
    if not os.path.exists(output_path):
        print(f"Downloading {url}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

def ingestdata(
        parquet_file,
        csv_file,
        engine,
        batch_size,
        parquet_table_name,
        csv_table_name):
    
    df_p=pd.read_parquet(parquet_file)

    df_csv=pd.read_csv(
        csv_file,
        dtype=dtype_csv_file,
    )

    df_p.head(0).to_sql(
        name='yellow_taxi_data',
        con=engine,
        if_exists='replace'
    )

    df_csv.head(0).to_sql(
        name='taxi_zone_lookup',
        con=engine,
        if_exists='replace'
    )

    for batch in tqdm(pq.ParquetFile(parquet_file).iter_batches(batch_size)):
        batch.to_pandas().to_sql(
            name=parquet_table_name,
            con=engine,
            if_exists='append'
        )
    
    print(f'The data has been inserted to {parquet_table_name}')


    df_csv.to_sql(
        name=csv_table_name,
        con=engine,
        if_exists='append'
    )

    print(f'The data has been inserted to {csv_table_name}')

@click.command()
@click.option("--pg-user", default="root", show_default=True)
@click.option("--pg-pass", default="root", show_default=True)
@click.option("--pg-host", default="localhost", show_default=True)
@click.option("--pg-db", default="ny_taxi", show_default=True)
@click.option("--pg-port", default=5432, type=int, show_default=True)
@click.option("--batch-size", default=10_000, type=int, show_default=True)
@click.option("--parquet-table-name", default="yellow_taxi_data", show_default=True)
@click.option("--csv-table-name", default="taxi_zone_lookup", show_default=True)
@click.option("--year", default=2025, type=int, show_default=True)
@click.option("--month", default=11, type=int, show_default=True)
def main(pg_user, pg_pass, pg_host, pg_db, pg_port, batch_size, parquet_table_name, csv_table_name,year, month):
    
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    if month == 1 or month > 12:
        raise ValueError("Month must be between 1 and 12")
    if month>= date.today().month and year==date.today().year:
        raise ValueError("Month must be less than the current month for the current year")
    if year < 2000 or year > date.today().year :
        raise ValueError(f"Year must be between 2000 and {date.today().year}")
    
    parquet_url = f"{parquet_url_prefix}green_tripdata_{year:04d}-{month:02d}.parquet"
    parquet_file = f"green_tripdata_{year:04d}-{month:02d}.parquet"
    csv_file = "taxi_zone_lookup.csv"

    download_file(parquet_url, parquet_file)
    download_file(csv_url, csv_file)

    ingestdata(
        parquet_file=parquet_file,
        csv_file=csv_file,
        engine=engine,
        batch_size=batch_size,
        parquet_table_name=parquet_table_name,
        csv_table_name=csv_table_name,
    )

if __name__=='__main__':
    main()