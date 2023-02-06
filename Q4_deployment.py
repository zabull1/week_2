from prefect.deployments import Deployment
from Q1_etl_web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("zoom-github-week2")

print(storage)

deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="Week 2 Question 4",
     storage=storage,
     entrypoint="week_2/Q1_etl_web_to_gcs.py:etl_web_to_gcs",
     parameters= {})

if __name__ == "__main__":
    deployment.apply()

# @task(retries=3)
# def fetch(dataset_url: str) -> pd.DataFrame:
#     """Read taxi data from web into pandas DataFrame"""
#     # if randint(0, 1) > 0:
#     #     raise Exception

#     df = pd.read_csv(dataset_url)
#     return df


# @task(log_prints=True)
# def clean(df: pd.DataFrame) -> pd.DataFrame:
#     """Fix dtype issues"""
#     df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
#     df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
#     print(df.head(2))
#     print(f"columns: {df.dtypes}")
#     print(f"rows: {len(df)}")
#     return df


# @task()
# def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
#     """Write DataFrame out locally as parquet file"""
#     path = Path(f"data/{color}/{dataset_file}.parquet")
#     df.to_parquet(path, compression="gzip")
#     return path


# @task()
# def write_gcs(path: Path) -> None:
#     """Upload local parquet file to GCS"""
#     gcs_block = GcsBucket.load("zoom-gcs")
#     gcs_block.upload_from_path(from_path=path, to_path=path)
#     return


# @flow()
# def etl_web_to_gcs() -> None:
#     """The main ETL function"""
#     color = "yellow"
#     year = 2021
#     month = 1
#     dataset_file = f"{color}_tripdata_{year}-{month:02}"
#     dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

#     df = fetch(dataset_url)
#     df_clean = clean(df)
#     path = write_local(df_clean, color, dataset_file)
#     # write_gcs(path)


# if __name__ == "__main__":
#     etl_web_to_gcs()
