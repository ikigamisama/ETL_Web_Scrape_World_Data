from functions.scrape.population import Population
from functions.scrape.gdp import GDP
from functions.scrape.co2 import CO2


class ScraperFactory:

    @staticmethod
    def create_scraper(scraper_type: str):
        factory = {
            "population": Population(),
            "gdp": GDP(),
            "co2": CO2(),
        }

        if scraper_type in factory:
            return factory[scraper_type]
        else:
            raise ValueError(f"Unknown scraper type: {scraper_type}")

    @staticmethod
    def get_available_scrapers():
        return ["population", "gdp"]
