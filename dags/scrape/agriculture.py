import pandas as pd

from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url
from functools import reduce


class Agriculture(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        agriculture_soup = await scrape_url(
            "https://www.worldometers.info/food-agriculture/",
            "#country-food--agriculture-profiles"
        )

        country_agriculture_df = await self._process_country_agricultre_data(
            agriculture_soup)

        self.save_to_s3(country_agriculture_df, self.bucket_name,
                        "agriculture/country_agriculture.csv")

    async def _process_country_agricultre_data(self, soup):
        agriculture_list = soup.find(
            'h2', id="country-food--agriculture-profiles").find_next_sibling('ul').find_all('a')

        country_agriculture_list = []
        for _, agri_link in enumerate(agriculture_list):
            link = "https://www.worldometers.info" + agri_link.get('href')
            country = agri_link.get_text()
            country_soup = await scrape_url(link, "#propscountrynamescommon-food--agriculture")

            Undernourished_People = None
            Undernourished_People_Percent = None
            Share_Global_Undernourished = None
            Forest_Hectares = None
            Share_World_Forest = None
            Crop_Area_Hectares = None
            Share_World_Crop_Area = None

            undernourished_header = country_soup.find(
                'h2', id="undernourished")
            card_data = None
            if undernourished_header:
                card_data = undernourished_header.find_next_sibling(
                    'div', class_="not-prose bg-white border rounded shadow-sm text-center max-w-xl mx-auto"
                )
            if card_data:
                undernourished_data = [data.get_text(
                    strip=True) for data in card_data.find_all('strong')]
                if len(undernourished_data) >= 3:
                    Undernourished_People = undernourished_data[0]
                    Undernourished_People_Percent = undernourished_data[1]
                    Share_Global_Undernourished = undernourished_data[2]

            # Forest section
            forest_header = country_soup.find('h2', id="forest")
            forest_card_data = None
            if forest_header:
                forest_card_data = forest_header.find_next_sibling(
                    'div', class_="not-prose bg-white border rounded shadow-sm text-center max-w-3xl mx-auto"
                )
            if forest_card_data:
                forest_data = [data.get_text(
                    strip=True) for data in forest_card_data.find_all('strong')]
                if len(forest_data) >= 2:
                    Forest_Hectares = forest_data[0]
                    Share_World_Forest = forest_data[1]

            # Cropland section
            crop_header = country_soup.find('h2', id="cropland")
            crop_card_data = None
            if crop_header:
                crop_card_data = crop_header.find_next_sibling(
                    'div', class_="not-prose bg-white border rounded shadow-sm text-center max-w-3xl mx-auto"
                )
            if crop_card_data:
                crop_data = [data.get_text(strip=True)
                             for data in crop_card_data.find_all('strong')]
                if len(crop_data) >= 2:
                    Crop_Area_Hectares = crop_data[0]
                    Share_World_Crop_Area = crop_data[1]

            country_agriculture_list.append({
                "Country": country,
                "Undernourished_People": Undernourished_People,
                "Undernourished_People_Percent": Undernourished_People_Percent,
                "Share_Global_Undernourished": Share_Global_Undernourished,
                "Forest_Hectares": Forest_Hectares,
                "Share_World_Forest": Share_World_Forest,
                "Crop_Area_Hectares": Crop_Area_Hectares,
                "Share_World_Crop_Area": Share_World_Crop_Area
            })

            number_undernourished_data = self.get_data_chart(
                country_soup, 'country-undernourished-chart')
            if number_undernourished_data is not None:
                number_undernourished_data = number_undernourished_data.rename(
                    columns={'people': "Undernourished_People"})
            else:
                number_undernourished_data = pd.DataFrame(
                    columns=["year", "Undernourished_People"])

            number_forest_area_data = self.get_data_chart(
                country_soup, 'country-forest-chart')
            if number_forest_area_data is not None:
                number_forest_area_data = number_forest_area_data.rename(
                    columns={'areaHectares': "Forest_Area_Hectares"})
            else:
                number_forest_area_data = pd.DataFrame(
                    columns=["year", "Forest_Area_Hectares"])

            number_cropland_area_data = self.get_data_chart(
                country_soup, 'country-cropland-chart')
            if number_cropland_area_data is not None:
                number_cropland_area_data = number_cropland_area_data.rename(
                    columns={'areaHectares': "Crop_Land_Area_Hectares"})
            else:
                number_cropland_area_data = pd.DataFrame(
                    columns=["year", "Crop_Land_Area_Hectares"])

            dfs = [number_undernourished_data,
                   number_forest_area_data, number_cropland_area_data]
            merge_df = reduce(lambda left, right: pd.merge(
                left, right, how="outer", on="year"), dfs)

            self.save_to_s3(merge_df, self.bucket_name,
                            f"agriculture/country/{country}.csv")

        return pd.DataFrame(country_agriculture_list)
