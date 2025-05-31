# 🌍 ETL_Web_Scrape_World_Data

**ETL_Web_Scrape_World_Data** is a web scraping ETL pipeline using **Apache Airflow 3**, **Docker**, and **MinIO** to automate the extraction and storage of global data from [Worldometers.info](https://www.worldometers.info/). It targets key metrics like CO₂ emissions, GDP, and population statistics.

---

## 📌 Overview

This pipeline scrapes HTML tables from Worldometers and stores the structured data into an S3-compatible MinIO bucket. The following datasets are collected:

- **CO₂ Emissions by Year**
  [https://www.worldometers.info/co2-emissions/co2-emissions-by-year/](https://www.worldometers.info/co2-emissions/co2-emissions-by-year/)

- **CO₂ Emissions by Country**
  [https://www.worldometers.info/co2-emissions/co2-emissions-by-country/](https://www.worldometers.info/co2-emissions/co2-emissions-by-country/)

- **World GDP**
  [https://www.worldometers.info/gdp/](https://www.worldometers.info/gdp/)

- **GDP by Country**
  [https://www.worldometers.info/gdp/gdp-by-country/](https://www.worldometers.info/gdp/gdp-by-country/)

- **World Population**
  [https://www.worldometers.info/world-population/](https://www.worldometers.info/world-population/)

- **Population by Country**
  [https://www.worldometers.info/world-population/population-by-country/](https://www.worldometers.info/world-population/population-by-country/)

---

## ⚙️ Tech Stack

- **Apache Airflow 3** – DAG orchestration
- **Python (asyncio, BeautifulSoup4)** – Scraping & ETL logic
- **Docker** – Containerized deployment
- **MinIO** – S3-compatible object storage
- **Pandas** – Data transformation

---

## 🗂️ Project Structure

```plaintext
ETL_Web_Scrape_World_Data/
├── dags/
│   └── etl_webscrape.py                # Airflow DAG definition
├── functions/
│   └── scrape/
│       ├── Factory.py                  # Scraper factory pattern
│       ├── co2.py                      # CO₂ scraping logic
│       ├── gdp.py                      # GDP scraping logic
│       └── population.py               # Population scraping logic
├── docker-compose.yml                 # Docker orchestration
├── requirements.txt                   # Python dependencies
└── README.md                          # Project documentation
```

---

## 🧠 DAG Logic

Below is the core DAG structure:

```python
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

    with TaskGroup('extract_data_scrape', tooltip="Extract the Data needed by WebScraping") as core_entity_transform_task:
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
```

---

## 📦 Output

Each dataset is exported to MinIO under:

```
s3://etl-world-statistics/
  ├── population/
  ├── gdp/
  └── co2/
```

---

## 🚀 Running the Project

1. **Clone the repository:**

```bash
git clone https://github.com/your-username/ETL_Web_Scrape_World_Data.git
cd ETL_Web_Scrape_World_Data
```

2. **Start services using Docker Compose:**

```bash
docker-compose up --build
```

3. **Access Airflow UI:**

- URL: [http://localhost:8080](http://localhost:8080)
- Credentials:

  - Username: `airflow`
  - Password: `airflow`

4. **Trigger DAG:**

Enable and trigger the `etl_webscrape` DAG to start the ETL process.

5. **Check MinIO Output:**

- URL: [http://localhost:9001](http://localhost:9001)
- Default credentials: `minioadmin` / `minioadmin`

---

## 📅 Future Expansion

The following blog URLs are planned for future scraping:

```python
blog_urls = [
    "https://www.worldometers.info/world-population/population-by-country/",  # Done
    "https://www.worldometers.info/gdp/gdp-by-country/",                      # Done
    "https://www.worldometers.info/co2-emissions/co2-emissions-by-country/",
    "https://www.worldometers.info/oil/oil-reserves-by-country/",
    "https://www.worldometers.info/oil/oil-consumption-by-country/",
    "https://www.worldometers.info/oil/oil-production-by-country/",
    "https://www.worldometers.info/coal/coal-reserves-by-country/",
    "https://www.worldometers.info/coal/coal-consumption-by-country/",
    "https://www.worldometers.info/coal/coal-production-by-country/",
    "https://www.worldometers.info/demographics/life-expectancy"
]
```

---

## ✍️ Author

Built by Ikigami
Feel free to contribute, fork, or star this repo!
