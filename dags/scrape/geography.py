import pandas as pd

from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_urls


class Geography(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):

        scrape_list = [
            ("https://www.worldometers.info/geography/flags-of-the-world/", ".not-prose"),
            ("https://www.worldometers.info/geography/largest-countries-in-the-world/",
             ".datatable-table"),
            ("https://www.worldometers.info/geography/countries-of-the-world/",
             ".datatable-table"),
        ]

        soup_country_geography = await scrape_urls(scrape_list)

        dfs = [
            self._process_flag_table(soup_country_geography[0]),
            self.get_data_from_table(soup_country_geography[1], {
                'Tot. Area (Km²)': 'Total_Area_KM',
                'Tot. Area (mi²)': 'Total_Area_Miles',
                'Land Area (Km²)': "Land_Area_KM",
                'Land Area (mi²)': 'Land_Area_Miles',
                'World Share': 'World_Share',
            }),
            self.get_data_from_table(soup_country_geography[2], {})
        ]

        geography_df = self._process_merge_df(dfs)

        self.save_to_s3(geography_df.drop(columns=['#_x', '#_y']), self.bucket_name,
                        "geography/world_geography.csv")

    def _process_flag_table(self, soup):
        flag_table = soup.find('div', class_="not-prose").find_all('div')
        img_flag = []
        country_flag = []
        for flag in flag_table:
            img_flag.append(self.base_url + flag.find('a').get('href'))
            country_flag.append(flag.find('span').get_text())

        return pd.DataFrame({'Flag_Img': img_flag, 'Country': country_flag})

    def _process_merge_df(self, dfs):
        flags_df, area_country_df, region_df = dfs
        return flags_df.merge(area_country_df, how="left", on="Country").merge(region_df, how="left", on="Country")
