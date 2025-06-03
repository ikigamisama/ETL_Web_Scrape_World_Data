from scrape import (
    Agriculture,
    CO2,
    Demographics,
    GDP,
    Population,
    Geography,
    Oil,
    Energy,
    Coal,
    Gas,
    Water
)


class ScraperFactory:
    @staticmethod
    def create_scraper(scraper_type: str):
        factory = {
            "population": Population(),
            "gdp": GDP(),
            "co2": CO2(),
            "demographics": Demographics(),
            "geography": Geography(),
            "oil": Oil(),
            "energy": Energy(),
            'coal': Coal(),
            'gas': Gas(),
            "water": Water(),
            "agriculture": Agriculture()
        }

        if scraper_type in factory:
            return factory[scraper_type]
        else:
            raise ValueError(f"Unknown scraper type: {scraper_type}")

    @staticmethod
    def get_available_scrapers():
        return ["population", "gdp", "co2", "demographics", "geography", "oil", "energy", "gas", "coal", 'water', "agriculture"]
