import pandas as pd

from _WorldStatistics import WorldStatistics
from functions.WebScraper import scrape_url


class CO2(WorldStatistics):
    def __init__(self):
        super().__init__()
