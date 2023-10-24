from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    # gcs_block = GcsBucket.load('zoom-gcs')
    # gcs_block.get_directory(from_path=gcs_path, local_path="../data/")
    return Path(f"data/{gcs_path}")

@task()
def prepare_df(path: Path) -> pd.DataFrame:
    """Load DataFrame"""    
    df = pd.read_parquet(path)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
       
    df.to_gbq(
        destination_table="dezoomcamp_ds.rides",
        project_id="datazoomcamp-prefect",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(log_prints=True)
def etl_gcs_to_bq(color, year, month):
    """Main ETL flow to load data into Big Quey"""
    path = extract_from_gcs(color, year, month)
    df = prepare_df(path)
    write_bq(df)
    rows = len(df)
    print(f'Rows Processed: {rows}')
    return rows

@flow(log_prints=True)
def parent_flow(color, year, months):
    rows_processed = 0
    for month in months:
        rows = etl_gcs_to_bq(color, year, month)
        rows_processed += rows
    print(rows_processed)


if __name__ == "__main__":
    color='yellow'
    year = 2019
    months = [2,3]
    parent_flow(color, year, months)
