from pathlib import Path
import pandas as pd
from prefect import flow, task
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import boto3



@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_cloud(path: Path) -> None:
    """Upload local parquet file to GCS"""
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    s3.upload_file(path.as_posix(), 'dtc-data-lake', path.as_posix())
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> int:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_cloud(path)
    return len(df_clean)



@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [2,3], year: int = 2019, color: str = "yellow"
):
    total_count=0
    for month in months:
        total_count+=etl_web_to_gcs(year, month, color)
    print(f"Total rows: {total_count}")


if __name__ == "__main__":
    color = "yellow"
    months = [2,3]
    year = 2019
    etl_parent_flow(months, year, color)
