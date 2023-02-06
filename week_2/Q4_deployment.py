from prefect.deployments import Deployment
from Q4_etl_web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("zoom-github-week2")

print(storage)

deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="Week 2 Question 4",
     storage=storage,
     entrypoint="week_2/Q4_etl_web_to_gcs.py:etl_web_to_gcs",
     parameters= {})

if __name__ == "__main__":
    deployment.apply()
