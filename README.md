# 🌍 ETL Web Scrape World Data

**ETL_Web_Scrape_World_Data** is a comprehensive web scraping ETL pipeline using **Apache Airflow 3**, **Docker**, and **MinIO** to automate the extraction and storage of global statistics from [Worldometers.info](https://www.worldometers.info/). This pipeline collects 11 different categories of world data including demographics, economics, energy, environment, and geographic information.

---

## 📌 Overview

This pipeline leverages a factory pattern to scrape HTML tables from Worldometers and stores structured data into an S3-compatible MinIO bucket. The system is designed for scalability and maintainability, allowing easy addition of new data sources.

### 📊 Data Categories Collected

The pipeline collects data from the following 11 categories:

#### 🏛️ **Population & Demographics**

- **Population**: Global and country-specific population statistics
- **Demographics**: Age distribution, birth/death rates (requires population data first)

#### 💰 **Economic Data**

- **GDP**: Gross Domestic Product by country and global trends

#### 🌱 **Environmental Data**

- **CO₂ Emissions**: Annual emissions and country-specific data
- **Water**: Global water resources and consumption

#### 🏔️ **Geographic Data**

- **Geography**: Country flags, largest countries, and geographic information

#### ⚡ **Energy & Resources**

- **Energy**: General energy consumption and production
- **Oil**: Production, consumption, and reserves by country
- **Gas**: Natural gas statistics and country data
- **Coal**: Coal production, consumption, and reserves

#### 🌾 **Agriculture**

- **Agriculture**: Food production and agricultural statistics

---

## 🗂️ Project Structure

```plaintext
ETL_Web_Scrape_World_Data/
├── dags/
│   └── etl_webscrape.py                # Airflow DAG definition
├── functions/                          # Core scraping functionality
│   └── scrape/                        # Scraper implementations
│       ├── Factory.py                  # Scraper factory pattern
│       ├── population.py              # Population data scraper
│       ├── gdp.py                     # GDP data scraper
│       ├── co2.py                     # CO₂ emissions scraper
│       ├── demographics.py            # Demographics scraper
│       ├── geography.py               # Geographic data scraper
│       ├── oil.py                     # Oil statistics scraper
│       ├── energy.py                  # Energy data scraper
│       ├── coal.py                    # Coal statistics scraper
│       ├── gas.py                     # Natural gas scraper
│       ├── water.py                   # Water resources scraper
│       └── agriculture.py             # Agriculture data scraper
├── docker-compose.yml                 # Docker orchestration
├── requirements.txt                   # Python dependencies
└── README.md                          # Project documentation
```

---

## 🏗️ Factory Pattern Implementation

The system uses a factory pattern for scalable scraper management:

```python
factory = {
    "population": Population(),
    "gdp": GDP(),
    "co2": CO2(),
    "demographics": Demographics(),
    "geography": Geography(),
    "oil": Oil(),
    "energy": Energy(),
    "coal": Coal(),
    "gas": Gas(),
    "water": Water(),
    "agriculture": Agriculture()
}
```

**Available scraper choices:**

```python
["population", "gdp", "co2", "demographics", "geography", "oil", "energy", "gas", "coal", "water", "agriculture"]
```

---

## 🌐 Data Sources

### Population Data

- **Main Source**: https://www.worldometers.info/world-population/
- **Country Data**: https://www.worldometers.info/world-population/population-by-country/

### GDP Data

- **Global GDP**: https://www.worldometers.info/gdp/
- **GDP by Country**: https://www.worldometers.info/gdp/gdp-by-country/

### CO₂ Emissions

- **Annual Emissions**: https://www.worldometers.info/co2-emissions/co2-emissions-by-year/
- **Country Emissions**: https://www.worldometers.info/co2-emissions/co2-emissions-by-country/

### Demographics

- **Note**: Requires population data to be scraped first as a dependency

### Geography

- **World Flags**: https://www.worldometers.info/geography/flags-of-the-world/
- **Largest Countries**: https://www.worldometers.info/geography/largest-countries-in-the-world/
- **Countries List**: https://www.worldometers.info/geography/countries-of-the-world/

### Oil Statistics

- **Oil Consumption**: https://www.worldometers.info/oil/oil-consumption-by-country/
- **Oil Production**: https://www.worldometers.info/oil/oil-production-by-country/
- **Oil Reserves**: https://www.worldometers.info/oil/oil-reserves-by-country/

### Energy Data

- **General Energy**: https://www.worldometers.info/energy/

### Natural Gas

- **Gas Statistics**: https://www.worldometers.info/gas/

### Coal Data

- **Coal Overview**: https://www.worldometers.info/coal/
- **Coal Production**: https://www.worldometers.info/coal/coal-production-by-country/
- **Coal Consumption**: https://www.worldometers.info/coal/coal-consumption-by-country/
- **Coal Reserves**: https://www.worldometers.info/coal/coal-reserves-by-country/

### Water Resources

- **Water Statistics**: https://www.worldometers.info/water/

### Agriculture

- **Food & Agriculture**: https://www.worldometers.info/food-agriculture/

---

## ⚙️ Tech Stack

- **Apache Airflow 3** – DAG orchestration and workflow management
- **Python** – Core programming language
- **Playwright** – Modern web scraping and browser automation
- **BeautifulSoup4** – HTML parsing and data extraction
- **Docker** – Containerized deployment and environment management
- **MinIO** – S3-compatible object storage
- **Pandas** – Data transformation and manipulation

---

## 🧠 Enhanced DAG Logic

The DAG now supports all 11 data categories with parallel processing:

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
    """Execute scraper for specified data type"""
    scraper = ScraperFactory.create_scraper(scraper_type)
    asyncio.run(scraper.scrape_and_save())

default_args = {
    'owner': 'ikigami',
    'depends_on_past': False,
    'retries': 2,
}

with DAG(
    "etl_webscrape",
    description="Comprehensive ETL pipeline for world statistics data",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
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
    create_s3_bucket >> extract_init
    extract_init >> [core_data_group, env_data_group, energy_data_group]
    population_task >> demographics_task  # Demographics requires population data
    core_data_group >> dependent_data_group
```

---

## 📦 Output Structure

Each dataset is organized in MinIO under structured paths:

```
s3://etl-world-statistics/
├── population/
│   ├── world_population.csv
│   └── population_by_country.csv
├── gdp/
│   ├── world_gdp.csv
│   └── gdp_by_country.csv
├── co2/
│   ├── co2_emissions_by_year.csv
│   └── co2_emissions_by_country.csv
├── demographics/
│   └── demographic_data.csv
├── geography/
│   ├── world_flags.csv
│   ├── largest_countries.csv
│   └── countries_list.csv
├── oil/
│   ├── oil_consumption.csv
│   ├── oil_production.csv
│   └── oil_reserves.csv
├── energy/
│   └── energy_statistics.csv
├── coal/
│   ├── coal_production.csv
│   ├── coal_consumption.csv
│   └── coal_reserves.csv
├── gas/
│   └── gas_statistics.csv
├── water/
│   └── water_resources.csv
└── agriculture/
    └── agriculture_data.csv
```

---

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose
- At least 4GB RAM allocated to Docker
- Internet connection for web scraping

### Installation & Setup

1. **Clone the repository:**

```bash
git clone https://github.com/your-username/ETL_Web_Scrape_World_Data.git
cd ETL_Web_Scrape_World_Data
```

2. **Start services using Docker Compose:**

```bash
docker-compose up --build -d
```

3. **Wait for services to initialize:**

```bash
# Check service status
docker-compose ps
```

4. **Access Airflow UI:**

- **URL**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

5. **Configure connections (if needed):**

- Navigate to Admin → Connections
- Verify AWS connection for MinIO

6. **Trigger the DAG:**

- Enable the `etl_webscrape` DAG
- Click "Trigger DAG" to start the ETL process

7. **Monitor progress:**

- Use the Airflow UI to monitor task execution
- Check logs for detailed information

8. **Access stored data:**

- **MinIO Console**: http://localhost:9001
- **Username**: `minioadmin`
- **Password**: `minioadmin`

---

## 🔧 Configuration

### Environment Variables

You can customize the pipeline behavior through environment variables:

```yaml
# In docker-compose.yml
environment:
  - MINIO_BUCKET=etl-world-statistics
  - SCRAPING_DELAY=2 # Delay between requests (seconds)
  - MAX_RETRIES=3 # Maximum retry attempts
```

### Adding New Scrapers

To add a new data source:

1. Create a new scraper class in `functions/scrape/`
2. Add it to the factory dictionary in `Factory.py`
3. Update the choices list
4. Add corresponding task in the DAG

---

## 📈 Monitoring & Maintenance

### Health Checks

- Airflow provides built-in monitoring through its web UI
- Check task success/failure rates
- Monitor data freshness and quality

### Data Quality

- Implement data validation in scraper classes
- Check for empty datasets or parsing errors
- Set up alerts for failed tasks

### Performance Optimization

- Adjust scraping delays to respect website limits
- Use parallel processing for independent data sources
- Monitor memory usage during large data extractions

---

## 🐛 Troubleshooting

### Common Issues

**1. Scraping Failures**

- Check if website structure has changed
- Verify internet connectivity
- Review scraper logs for specific errors

**2. MinIO Connection Issues**

- Ensure MinIO service is running
- Check AWS connection configuration in Airflow
- Verify bucket permissions

**3. Task Dependencies**

- Demographics scraper requires population data first
- Check task order in DAG execution

**4. Memory Issues**

- Increase Docker memory allocation
- Optimize data processing in scrapers
- Process data in smaller chunks

---

## 🤝 Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

### Development Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Format code
black functions/
```

---

## 📄 License

This project is licensed under the MIT License. See the LICENSE file for details.

---

## ✍️ Author

**Built by Ikigami**

Feel free to contribute, fork, or star this repository!

For questions or suggestions, please open an issue or reach out through GitHub.

---

## 🙏 Acknowledgments

- [Worldometers.info](https://www.worldometers.info/) for providing comprehensive world statistics
- Apache Airflow community for the excellent orchestration platform
- Docker and MinIO teams for containerization and storage solutions
