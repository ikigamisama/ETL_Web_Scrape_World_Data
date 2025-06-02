import pandas as pd

from loguru import logger
from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class Population(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        """Scrape population data and save to s3"""

        world_population_table = await scrape_url(
            "https://www.worldometers.info/world-population/",
            ".datatable-container"
        )

        population_country_table = await scrape_url(
            "https://www.worldometers.info/world-population/population-by-country/",
            ".datatable-container"
        )

        country_df = self._process_country_population(population_country_table)
        # self.save_to_s3(country_df, self.bucket_name,"population/country_population.csv")
        self.save_csv(country_df, "population/country_population.csv")

        world_df = self.get_data_from_table(
            world_population_table, {
                'Year(July 1)': 'Year',
                'Population': 'Population',
                'Yearly %Change': "Yearly_Change_Percent",
                "YearlyChange": "Yearly_Change",
                "MedianAge": "Median_Age",
                "FertilityRate": "Fertility_Rate",
                "Density(P/Km²)": "Density_Area",
            }, 0)
        # self.save_to_s3(world_df, self.bucket_name,"population/world_population.csv")
        self.save_csv(world_df, "population/world_population.csv")

        world_forecast_df = self.get_data_from_table(
            world_population_table, {
                'Year(July 1)': 'Year',
                'Population': 'Population',
                'Yearly %Change': "Yearly_Change_Percent",
                "YearlyChange": "Yearly_Change",
                "MedianAge": "Median_Age",
                "FertilityRate": "Fertility_Rate",
                "Density(P/Km²)": "Density_Area",
            }, 1)
        # self.save_to_s3(world_forecast_df, self.bucket_name,"population/world_population_forecast.csv")
        self.save_csv(world_forecast_df,
                      "population/world_population_forecast.csv")

        await self._process_individual_countries(country_df)

        print("Population data scraping completed!")

    def _process_country_population(self, soup):
        """Process country population table with links"""
        rows = []

        for i, tr in enumerate(soup.find('table').find_all("tr")):
            cells = tr.find_all(["td", "th"])

            if i == 0:
                row = []
                for cell in cells:
                    text = cell.get_text(strip=True)
                    row.append(text)
                    if "Country" in text:
                        row.append("Link")
            else:
                row = []
                for j, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    a_tag = cell.find("a")
                    if j == 1 and a_tag and a_tag.get("href"):
                        row.append(text)
                        row.append(self.base_url + a_tag["href"])
                    else:
                        row.append(text)

            rows.append(row)

        df = self.convert_to_dataframe(rows)
        df = df.rename(columns={
            '#': 'Rank',
            'Country (ordependency)': 'Country',
            'Link': 'Link',
            'Population (2025)': 'Population_2025',
            'YearlyChange': "Yearly_Change",
            "NetChange": "Net_Change",
            "Density(P/Km²)": "Density_Area",
            "Land Area(Km²)": "Land_Area",
            "Migrants(net)": "Migrants",
            "Fert.Rate": "Fertility_Rate",
            "MedianAge": "Median_Age",
            "UrbanPop %": "Urban_Population_Percent",
            "WorldShare": "World_Share_Percentage"
        })

        return df

    async def _process_individual_countries(self, country_df):
        """Process individual country data (first 3 countries)"""
        demographics_country = []

        for i, row in country_df.iterrows():
            link = row['Link']
            country = row['Country']

            country_population_soup = await scrape_url(link, ".datatable-table")

            country_df_current = self._process_country_table(
                country_population_soup, table_index=0
            )
            # self.save_to_s3(
            #     country_df_current,
            #     self.bucket_name,
            #     f"population/country/{country}.csv"
            # )

            self.save_csv(country_df_current,
                          f"population/country/{country}.csv")

            country_df_forecast = self._process_country_table(
                country_population_soup, table_index=1
            )
            # self.save_to_s3(
            #     country_df_forecast,
            #     self.bucket_name,
            #     f"population/country/{country}_forecast.csv"
            # )

            self.save_csv(country_df_forecast,
                          f"population/country/{country}_forecast.csv")

            try:
                demographics_tag = country_population_soup.find(
                    "h1", id="propscountrynamessimple-demographics")
                next_div = demographics_tag.find_next_sibling("div").find('a')

                demographics_country.append({
                    'country': country,
                    'url': self.base_url + next_div.get('href')
                })
            except:
                logger.info(f"No demographics data found for {country}")

        demographics_df = pd.DataFrame(demographics_country)
        # self.save_to_s3(
        #     demographics_df, self.bucket_name, "population/country_demographics.csv")

        self.save_csv(demographics_df,
                      "population/demographics/country_demographics.csv")

    def _process_country_table(self, soup, table_index=0):
        """Process individual country table"""

        df = self.get_data_from_table(soup, {
            'Year': 'Year',
            'Population': 'Population',
            'Yearly %Change': 'Yearly_Change_Percent',
            'YearlyChange': 'Yearly_Change',
            'Migrants(net)': "Migrants",
            "MedianAge": "Median_Age",
            "FertilityRate": "Fertility_Rate",
            "Density(P/Km²)": "Density_Area",
            "UrbanPop %": "Urban_Population_Percentage",
            "UrbanPopulation": "Urban_Population",
            "WorldPopulation": "World_Population",
            "Country'sShare ofWorld Pop": "Country_Share_World_Pop_Percentage",
        }, table_index)

        for col in df.columns:
            if col.endswith('GlobalRank'):
                df = df.rename(columns={col: 'Global_Rank'})
                break

        return df
