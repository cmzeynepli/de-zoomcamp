"""NYC taxi trips REST API ingestion with dlt."""

from __future__ import annotations

import dlt
from dlt.sources.helpers import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.resource(name="nyc_taxi_trips", write_disposition="append")
def nyc_taxi_trips(page_size: int = 1000):
    """Fetch NYC taxi trip records page-by-page until an empty page is returned."""
    page = 1
    while True:
        response = requests.get(
            BASE_URL,
            params={"page": page, "page_size": page_size},
            timeout=60,
        )
        response.raise_for_status()

        payload = response.json()
        records = payload.get("data") if isinstance(payload, dict) else payload

        if not records:
            break

        if not isinstance(records, list):
            raise TypeError(f"Expected list of records, got: {type(records).__name__}")

        yield records
        page += 1


@dlt.source(name="taxi_rest_api")
def taxi_rest_api_source(page_size: int = 1000):
    return nyc_taxi_trips(page_size=page_size)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_rest_api_source())
    print(load_info)  # noqa: T201

