import pandas as pd
from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url, scrape_urls


class Oil(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        scrape_list = [
            ("https://www.worldometers.info/oil/oil-consumption-by-country/",
             ".datatable-table"),
            ("https://www.worldometers.info/oil/oil-production-by-country/",
                ".datatable-table"),
            ("https://www.worldometers.info/oil/oil-reserves-by-country/",
                ".datatable-table"),
        ]

        soup_country_oil = await scrape_urls(scrape_list)

        dfs = [
            self.get_data_from_table(soup_country_oil[0], {
                'Daily OilConsumption(barrels)': "Daily_Oil_Consumption_Barrels",
                'WorldShare': "World_Share",
                'Yearly GallonsPer Capita': "Yearly_Gallons_Per_Capita",
            }),
            self.get_data_from_table(soup_country_oil[1], {
                'Yearly Oil Production(Barrels per day)': "Yearly_Oil_Production_Barrels_Per_Day",
            }),
            self.get_data_from_table(soup_country_oil[2], {
                'Oil Reserves(barrels) in 2016': "Oil_Reserves_Barrels",
                'WorldShare': "World_Share",
            })
        ]

        oil_df = self._process_merge_df(dfs)
        self.save_csv(oil_df.drop(columns=['#_x', '#_y', '#', 'World_Share_y']).rename(
            columns={'World_Share_x': "World_Share"}), "oil/world_oil.csv")

        await self._process_country_oil_details(soup_country_oil[0])

    async def _process_country_oil_details(self, soup):
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

            country_oil_details_soup = await scrape_url(link, ".not-prose")

            reserves_oil_df = self.get_data_chart(
                country_oil_details_soup, 'country-oil-reserves-chart')
            prod_oil_df = self.get_data_chart(
                country_oil_details_soup, 'country-oil-consum-prod-chart')

            if (
                reserves_oil_df is not None and not reserves_oil_df.empty and
                prod_oil_df is not None and not prod_oil_df.empty
            ):
                oil_country_df = pd.merge(
                    reserves_oil_df, prod_oil_df, how="left", on="year")
                self.save_csv(oil_country_df, f"oil/country/{Country}.csv")

            else:
                if reserves_oil_df is not None and not reserves_oil_df.empty:
                    reserves_oil_df['production'] = None
                    reserves_oil_df['consumption'] = None
                    self.save_csv(reserves_oil_df,
                                  f"oil/country/{Country}.csv")

                if prod_oil_df is not None and not prod_oil_df.empty:
                    prod_oil_df['reserves'] = None
                    self.save_csv(
                        prod_oil_df, f"oil/country/{Country}.csv")

    def _process_merge_df(self, dfs):
        consumption_df, production_df, reserve_df = dfs
        return consumption_df.merge(production_df, how="left", on="Country").merge(reserve_df, how="left", on="Country")
