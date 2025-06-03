import asyncio
from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

from functions.factory import ScraperFactory


def run_scraper(scraper_type: str):
    scraper = ScraperFactory.create_scraper(scraper_type)
    asyncio.run(scraper.scrape_and_save())


default_args = {
    'owner': 'ikigami',
    'depends_on_past': False,
}


with DAG(
    "etl_webscrape",
    description="Comprehensive ETL pipeline for world statistics data",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    tags=['ETL', 'WebScrape', 'WorldData', 'Statistics'],
    catchup=False,
) as dag:

    analytics_bucket = "etl-world-statistics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name=analytics_bucket,
        aws_conn_id="aws_default"
    )

    extract_init = EmptyOperator(task_id="extract_init")

    with TaskGroup('extract_core_data', tooltip="Extract core demographic and economic data") as core_data_group:
        population_task = PythonOperator(
            task_id='population_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'population'}
        )

        gdp_task = PythonOperator(
            task_id='gdp_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'gdp'}
        )

        geography_task = PythonOperator(
            task_id='geography_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'geography'}
        )

    with TaskGroup('extract_environmental_data', tooltip="Extract environmental and climate data") as env_data_group:
        co2_task = PythonOperator(
            task_id='co2_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'co2'}
        )

        water_task = PythonOperator(
            task_id='water_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'water'}
        )

    with TaskGroup('extract_energy_data', tooltip="Extract energy and resource data") as energy_data_group:
        oil_task = PythonOperator(
            task_id='oil_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'oil'}
        )

        energy_task = PythonOperator(
            task_id='energy_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'energy'}
        )

        coal_task = PythonOperator(
            task_id='coal_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'coal'}
        )

        gas_task = PythonOperator(
            task_id='gas_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'gas'}
        )

    with TaskGroup('extract_dependent_data', tooltip="Extract data requiring dependencies") as dependent_data_group:
        demographics_task = PythonOperator(
            task_id='demographics_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'demographics'}
        )

        agriculture_task = PythonOperator(
            task_id='agriculture_data',
            python_callable=run_scraper,
            op_kwargs={'scraper_type': 'agriculture'}
        )

    # Dependencies
    create_s3_bucket >> extract_init >> core_data_group >> env_data_group >> energy_data_group >> dependent_data_group
