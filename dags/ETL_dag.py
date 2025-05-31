import asyncio
from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

from functions.scrape.Factory import ScraperFactory


def run_scraper(scraper_type: str):
    scraper = ScraperFactory.create_scraper(scraper_type)
    asyncio.run(scraper.scrape_and_save())


default_args = {
    'owner': 'ikigami',
    'depends_on_past': False,
}


with DAG(
    "etl_webscrape",
    description="A DAG to execute ETL Webscraping World Statistics Data",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    tags=['ETL', 'WebScrape', 'Data'],
    catchup=False,
) as dag:
    analytics_bucket = "etl-world-statistics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=analytics_bucket, aws_conn_id="aws_default"
    )

    extract_init = EmptyOperator(task_id="extract_init")

    with TaskGroup('extract_data_scrape', tooltip="Extrract the Data neede by WebScraping") as core_entity_transform_task:
        population_data = PythonOperator(
            task_id='population_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'population'}
        )

        gdp_data = PythonOperator(
            task_id='gdp_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'gdp'}
        )

        co2_data = PythonOperator(
            task_id='co2_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'co2'}
        )

    create_s3_bucket >> extract_init >> core_entity_transform_task
