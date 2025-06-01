import os
import pandas as pd

from loguru import logger
from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class Demographics(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        demographics_data = './data/population/demographics/country_demographics.csv'
        if os.path.exists(demographics_data):
            country_demographics_df = pd.read_csv(demographics_data)

            await self._process_demographics_data(country_demographics_df)
        else:
            return logger.error("Cannot Process Scrape Demographics as the Country Demographics not yet created ")

    async def _process_demographics_data(self, country_df):
        for i, row in country_df.iterrows():
            url = row['url']
            country = row['country']

            demographics_soup = await scrape_url(url, '.datatable-container')
            data_expectancy = self.get_data_chart(
                demographics_soup, 'country-life-expectancy-chart', False)

            life_expanded = data_expectancy['lifeExpectancy'].apply(pd.Series)
            life_expanded.columns = [
                f'Life_Expectancy_{col}' for col in life_expanded.columns]

            mortality_expanded = data_expectancy['mortalityRate'].apply(
                pd.Series)
            mortality_expanded.columns = [
                f'Mortality_Rate_{col}' for col in mortality_expanded.columns]

            data_expectancy = data_expectancy.drop(columns=['lifeExpectancy',
                                                            'mortalityRate', '__typename'])
            expectancy_df = pd.concat(
                [data_expectancy, life_expanded, mortality_expanded], axis=1)

            urban_population = self.get_data_chart(
                demographics_soup, 'country-urban-population-chart', False)
            urban_population.loc[:, 'rural'] = urban_population['total'] - \
                urban_population['urban']
            urban_population['urban'] = urban_population['urban'].fillna(
                urban_population['total'])

            demographics_df = pd.merge(expectancy_df, urban_population[[
                                       'year', 'urban', 'rural']], how="inner", on="year")

            self.save_csv(demographics_df,
                          f"population/demographics/country/{country}.csv")
