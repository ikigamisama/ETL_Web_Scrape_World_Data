import pandas as pd

from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class Gas(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        gas_world_soup = await scrape_url("https://www.worldometers.info/gas/", ".datatable-table")

        gas_reserve_world_df = self.get_data_chart(
            gas_world_soup, 'world-gas-reserves-chart')
        gas_consumption_world_df = self.get_data_chart(
            gas_world_soup, 'world-gas-consum-prod-chart')

        world_gas_df = pd.merge(
            gas_reserve_world_df, gas_consumption_world_df, how="left", on="year")

        self.save_to_s3(world_gas_df, self.bucket_name, "gas/world_gas.csv")

        country_gas_df = self.get_data_from_table(gas_world_soup, {
            'Gas Reserves(MMcf)': 'Gas_Reserves_MMCF',
            'WorldShare': "World_Share"
        })

        self.save_to_s3(country_gas_df, self.bucket_name,
                        "gas/country_gas.csv")

        await self.__process_country_gas_table(gas_world_soup)

    async def __process_country_gas_table(self, soup):
        table = soup.find_all('table', class_="datatable-table")[0]

        links = []
        country = []
        for tr in table.find_all("tr"):
            for td in tr.find_all("td"):
                if td.find('a') is not None:
                    links.append(self.base_url + td.find('a').get('href'))
                    country.append(td.find('a').get_text())

        country_df = pd.DataFrame({'Link': links, 'Country': country})

        for i, row in country_df.iterrows():
            link = row['Link']
            Country = row['Country']

            country_gas_details_soup = await scrape_url(link, ".not-prose")

            reserves_gas_df = self.get_data_chart(
                country_gas_details_soup, 'country-gas-reserves-chart')
            prod_gas_df = self.get_data_chart(
                country_gas_details_soup, 'country-gas-consum-prod-chart')

            if (
                reserves_gas_df is not None and not reserves_gas_df.empty and
                prod_gas_df is not None and not prod_gas_df.empty
            ):
                oil_country_df = pd.merge(
                    reserves_gas_df, prod_gas_df, how="left", on="year")

                self.save_to_s3(oil_country_df, self.bucket_name,
                                f"gas/country/{Country}.csv")

            else:
                if reserves_gas_df is not None and not reserves_gas_df.empty:
                    reserves_gas_df['production'] = None
                    reserves_gas_df['consumption'] = None

                    self.save_to_s3(reserves_gas_df, self.bucket_name,
                                    f"gas/country/{Country}.csv")

                if prod_gas_df is not None and not prod_gas_df.empty:
                    prod_gas_df['reserves'] = None

                    self.save_to_s3(prod_gas_df, self.bucket_name,
                                    f"gas/country/{Country}.csv")
