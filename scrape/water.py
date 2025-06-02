from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class Water(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        pass
