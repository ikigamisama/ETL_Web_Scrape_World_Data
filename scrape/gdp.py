import pandas as pd

from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class GDP(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):

        world_gdp_table = await scrape_url("https://www.worldometers.info/gdp/", ".datatable-container")
        gdp_country_table = await scrape_url("https://www.worldometers.info/gdp/gdp-by-country/", ".datatable-container")

        gdp_world_df = self._process_world_GDP(world_gdp_table, 0, {
            'Year': 'Year',
            'GDP Real(Inflation adj.)': 'GDP_Inflation_Adjust',
            'GDPGrowth': "GDP_Growth",
            "PerCapita": "Per_Capita",
            "GDP Nominal(Current USD)": "GDP_Nominal_USD",
            "Pop.Change": "Population_Change",
            "WorldPopulation": "World_Population",
        })
        # self.save_to_s3(gdp_world_df, self.bucket_name, "gdp/world_GDP.csv")
        self.save_csv(gdp_world_df, "gdp/world_GDP.csv")

        gdp_world_region_df = self._process_world_GDP(world_gdp_table, 1, {
            'Region': 'Region',
            'GDP(nominal, 2023)': 'GDP_Nominal',
            'GDPGrowth': "GDP_Growth",
            "Share ofWorld GDP": "Share_World_GDP",
        })
        # self.save_to_s3(gdp_world_region_df,self.bucket_name, "gdp/world_GDP_region.csv")
        self.save_csv(gdp_world_region_df, "gdp/world_GDP_region.csv")

        gdp_country_df = self._process_country_GDP(gdp_country_table)
        # self.save_to_s3(gdp_country_df, self.bucket_name,"gdp/country_population.csv")
        self.save_csv(gdp_country_df, "gdp/country_population.csv")

        await self._process_individual_country_GDP(gdp_country_df)

        print("GDP data scraping completed!")

    def _process_world_GDP(self, soup, index, columns) -> pd.DataFrame:
        rows = []
        table = soup.find_all('table', class_="datatable")[index]

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            row = [cell.get_text(strip=True) for cell in cells]
            rows.append(row)

        df = self.convert_to_dataframe(rows)

        df = df.rename(columns=columns)
        return df

    def _process_country_GDP(self, soup) -> pd.DataFrame:
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
            'Country (or dependency)': 'Country',
            'Link': 'Link',
            'GDP(nominal, 2023)': 'GDP_Nominal',
            'GDP(abbrev.)': "GDP_Abbrev",
            "GDPGrowth": "GDP_Growth",
            "Population(2023)": "Population",
            "GDPpercapita": "GDP_Per_Capita",
            "Share ofWorld GDP": "Share_World_GDP",
        })

        return df

    async def _process_individual_country_GDP(self, country_df):
        for i, row in country_df.iterrows():
            link = row['Link']
            country = row['Country']

            country_population_soup = await scrape_url(link, ".datatable-table")
            country_individual_df = self._process_country_table(
                country_population_soup, table_index=0
            )
            # self.save_to_s3(
            #     country_individual_df,
            #     self.bucket_name,
            #     f"gdp/country/{country}.csv"
            # )

            self.save_csv(country_individual_df, f"gdp/country/{country}.csv")

    def _process_country_table(self, soup, table_index=0):
        """Process individual country table"""
        rows = []
        table = soup.find_all('table', class_="datatable")[table_index]

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            row = [cell.get_text(strip=True) for cell in cells]
            rows.append(row)

        df = self.convert_to_dataframe(rows)
        df = df.rename(columns={
            'Year': 'Year',
            'GDP Nominal(Current USD)': 'GDP_Nominal',
            'GDP Real(Inflation adj.)': 'GDP_Real_Inflation_Adjust',
            'GDPChange': 'GDP_Change',
            'GDPpercapita': "GDP_per_capita",
            "Pop.Change": "Population_Change",
        })

        return df
