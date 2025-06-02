import os
import sys
import asyncio
import argparse
import datetime as dt

from loguru import logger
from scrape import CO2, Demographics, GDP, Population, Geography, Oil

PROGRAM_NAME = "World o Meters Data Scrapper"


class ScraperFactory:
    @staticmethod
    def create_scraper(scraper_type: str):
        factory = {
            "population": Population(),
            "gdp": GDP(),
            "co2": CO2(),
            "demographics": Demographics(),
            "geography": Geography(),
            "oil": Oil()
        }

        if scraper_type in factory:
            return factory[scraper_type]
        else:
            raise ValueError(f"Unknown scraper type: {scraper_type}")

    @staticmethod
    def get_available_scrapers():
        return ["population", "gdp", "co2", "demographics", "geography", "oil"]

    @staticmethod
    def init_folder(scraper_type: str):
        base_paths = {
            "population": [
                "./data/population",
                "./data/population/country/",
                "./data/population/demographics/",
            ],
            "gdp": [
                "./data/gdp",
                "./data/gdp/country",
            ],
            "co2": [
                "./data/co2",
                "./data/co2/country",
            ],
            "demographics": [
                "./data/population/demographics/country",
            ],
            "geography": [
                "./data/geography/",
            ],
            "oil": [
                "./data/oil/",
                "./data/oil/country",
            ]
        }

        paths = base_paths.get(scraper_type, [])
        for path in paths:
            try:
                os.makedirs(path, exist_ok=True)
                logger.info(f"Directory created or already exists: {path}")
            except Exception as e:
                logger.error(f"Failed to create directory {path}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a specific scraper.")
    parser.add_argument(
        "--scraper_type",
        choices=ScraperFactory.get_available_scrapers(),
        required=True,
        help="Type of scraper to run (population, gdp, co2)",
    )
    args = parser.parse_args()

    start_time = dt.datetime.now()

    logger.remove()
    logger.add("logs/std_out.log", rotation="10 MB", level="INFO")
    logger.add("logs/std_err.log", rotation="10 MB", level="ERROR")
    logger.add(sys.stdout, level="INFO")
    logger.add(sys.stderr, level="ERROR")

    logger.info(f"{PROGRAM_NAME} has started")

    scraper = ScraperFactory.create_scraper(args.scraper_type)
    ScraperFactory.init_folder(args.scraper_type)
    asyncio.run(scraper.scrape_and_save())

    end_time = dt.datetime.now()
    duration = end_time - start_time
    logger.info(
        f"{PROGRAM_NAME} (category={args.scraper_type}) has ended. Elapsed: {duration}")
