import io
import pandas as pd

from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class Demographics(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key='population/demographics/country_demographics.csv')
            body = response["Body"].read()
            country_demographics_df = pd.read_csv(io.BytesIO(body))

            await self._process_demographics_data(country_demographics_df)
        except self.s3_client.exceptions.NoSuchKey:
            print(
                "Cannot Process Scrape Demographics as the Country Demographics not yet created")
        except Exception as e:
            print(f"Failed to load demographics data from S3: {e}")

    async def _process_demographics_data(self, country_df):
        for _, row in country_df.iterrows():
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

            self.save_to_s3(demographics_df, self.bucket_name,
                            f"population/demographics/country/{country}.csv")
