from _Population import Population
from _GDP import GDP


class ScraperFactory:

    @staticmethod
    def create_scraper(scraper_type: str):
        if scraper_type.lower() == "population":
            return Population()
        elif scraper_type.lower() == "gdp":
            return GDP()
        else:
            raise ValueError(f"Unknown scraper type: {scraper_type}")

    @staticmethod
    def get_available_scrapers():
        return ["population", "gdp"]
