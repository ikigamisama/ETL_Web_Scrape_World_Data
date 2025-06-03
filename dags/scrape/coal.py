import pandas as pd

from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url, scrape_urls


class Coal(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        coal_world_soup = await scrape_url("https://www.worldometers.info/coal/", ".datatable-table")
        scrape_list = [
            ("https://www.worldometers.info/coal/coal-production-by-country/",
                ".datatable-table"),
            ("https://www.worldometers.info/coal/coal-consumption-by-country/",
                ".datatable-table"),
            ("https://www.worldometers.info/coal/coal-reserves-by-country/",
                ".datatable-table"),
        ]

        soup_country_coal = await scrape_urls(scrape_list)

        coal_reserve_world_df = self.get_data_chart(
            coal_world_soup, 'country-coal-reserves-chart')
        coal_consumption_world_df = self.get_data_chart(
            coal_world_soup, 'world-coal-consum-prod-chart')

        world_coal_df = pd.merge(
            coal_reserve_world_df, coal_consumption_world_df, how="right", on="year")

        self.save_to_s3(world_coal_df, self.bucket_name, "coal/world_coal.csv")

        world_coal_data_df = self._process_merge_coal_table(soup_country_coal)
        self.save_to_s3(world_coal_data_df, self.bucket_name,
                        "coal/country_coal.csv")

        await self.__process_country_coal_table(soup_country_coal[1])

    def _process_merge_coal_table(self, dfs):
        coal_production, coal_consumption, coal_reserves = dfs

        coal_production_df = self.get_data_from_table(coal_production, {
            'Yearly Coal Production(Tons)': 'Yearly_Coal_Production_Tons'
        }).drop(columns=['#'])

        coal_consumption_df = self.get_data_from_table(coal_consumption, {
            'Yearly CoalConsumption(tons)': 'Yearly_Coal_Consumption_Tons',
            'WorldShare': 'World_Share_Consumption',
            'Cubic FeetPer Capita': 'Cubic_Feet_Per_Capita'
        }).drop(columns=['#'])

        coal_reserves_df = self.get_data_from_table(coal_reserves, {
            'Coal Reserves(tons) in 2016': 'Coal_Reserves_Tons_2016',
            'WorldShare': 'World_Share_Reserves'
        }).drop(columns=['#'])

        merge_df = coal_production_df.merge(coal_consumption_df, how="right", on="Country").merge(
            coal_reserves_df, how="left", on="Country")

        return merge_df

    async def __process_country_coal_table(self, soup):
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

            country_coal_details_soup = await scrape_url(link, ".not-prose")

            reserves_coal_df = self.get_data_chart(
                country_coal_details_soup, 'country-coal-reserves-chart')
            prod_coal_df = self.get_data_chart(
                country_coal_details_soup, 'country-coal-consum-prod-chart')

            if (
                reserves_coal_df is not None and not reserves_coal_df.empty and
                prod_coal_df is not None and not prod_coal_df.empty
            ):
                oil_country_df = pd.merge(
                    reserves_coal_df, prod_coal_df, how="left", on="year")

                self.save_to_s3(oil_country_df, self.bucket_name,
                                f"coal/country/{Country}.csv")

            else:
                if reserves_coal_df is not None and not reserves_coal_df.empty:
                    reserves_coal_df['production'] = None
                    reserves_coal_df['consumption'] = None

                    self.save_to_s3(reserves_coal_df, self.bucket_name,
                                    f"coal/country/{Country}.csv")

                if prod_coal_df is not None and not prod_coal_df.empty:
                    prod_coal_df['reserves'] = None

                    self.save_to_s3(prod_coal_df, self.bucket_name,
                                    f"coal/country/{Country}.csv")
