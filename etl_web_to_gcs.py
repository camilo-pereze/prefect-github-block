import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3)
def fetch(dataset_url:str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)   
    return df

@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"Columns: {df.dtypes}")
    print(f"Rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path_dir = Path(f"data/{color}")
    path = Path(f"{path_dir}/{dataset_file}.parquet")
    if not os.path.exists(path_dir):
        os.makedirs(path_dir)   
    df.to_parquet(path, compression="gzip")
    return path

# @task()
# def write_gcs(path: Path) -> None:
#     """Uploading local parquet file to GCS"""
#     gcp_bucket_block = GcsBucket.load("zoom-gcs")
#     gcp_bucket_block.upload_from_path(
#         from_path=path,
#         to_path=path,
#         timeout=600

#     ) 



@flow()
def etl_web_to_gcs(color, year, month) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df, color, dataset_file)
    # write_gcs(path)

@flow()
def parent_flow(params:list):
    for color, year, month in params:
        etl_web_to_gcs(color, year, month)

if __name__ == '__main__':
    params = [['green', 2020, 11]]
    parent_flow(params)