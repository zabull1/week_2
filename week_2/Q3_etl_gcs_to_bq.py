from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    folder_path = f"data/{color}/"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gap")
    gcs_block.get_directory(from_path=gcs_path, local_path= gcs_path)
    return Path(f"{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    # """Data cleaning example"""
    df = pd.read_parquet(path)
    count = len(df)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

    return df, count


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gap-creds")

    df.to_gbq(
        destination_table="dezoomcamp.yellow_taxi_data",
        project_id="deft-citizen-374601",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints= True)
def etl_gcs_to_bq(  year: int, month: int ,color: str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df,count = transform(path)
    write_bq(df)
    return count

@flow(log_prints= True)
def etl_parent_flow( months: list[int], year: int, color: str,count: int):
    for month in months:
        count += etl_gcs_to_bq(year, month, color)
    print(f"rows processed: {count}")

if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    count = 0
    etl_parent_flow(months, year, color, count)
