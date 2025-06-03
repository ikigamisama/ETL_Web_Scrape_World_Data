from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class CO2(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):

        world_co2_emission_table = await scrape_url(
            "https://www.worldometers.info/co2-emissions/co2-emissions-by-year/",
            ".datatable-container"
        )

        country_co2_emisstion_table = await scrape_url(
            "https://www.worldometers.info/co2-emissions/co2-emissions-by-country/",
            ".datatable-container"
        )

        world_df = self.get_data_from_table(world_co2_emission_table, {
            'Year': 'Year',
            'CO2emissions(tons, 2022)': 'CO2_Emission',
            '1 YearChange': "Yearly_Change",
            "Per Capita": "Per_Capita",
            "Median Age": "Median_Age",
            "Population": "Population",
            "Pop.change": "Population_Change",
        })
        self.save_to_s3(world_df, self.bucket_name, "co2/world_co2.csv")

        country_df = self._process_country_co2(country_co2_emisstion_table)
        self.save_to_s3(country_df, self.bucket_name, "co2/country_co2.csv")

        await self._process_individual_countries(country_df)

    def _process_country_co2(self, soup):
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

        # Rename columns
        df = df.rename(columns={
            '#': 'Rank',
            'Country': 'Country',
            'Link': 'Link',
            'CO2emissions(tons, 2022)': 'CO2_Emission',
            '1 YearChange': "Yearly_Change",
            'Population(2022)': "Population_2022",
            "Per Capita": "Per_Capita",
            "Share ofWorld": "Share_World_Percentage",
        })

        return df

    async def _process_individual_countries(self, country_df):
        """Process individual country data (first 3 countries)"""

        for i, row in country_df.iterrows():
            link = row['Link']
            country = row['Country']

            country_co2_soup = await scrape_url(link, ".datatable-table")

            country_df_current = self.get_data_from_table(
                country_co2_soup, {
                    'Year': 'Year',
                    'Fossil CO2emissions(tons)': 'Fossil_CO2_Emission',
                    'CO2 emissionschange': "CO2_Emission_Change",
                    'CO2 emissionsper capita': "CO2_Emission_Capita",
                    "Pop.change": "Population_Change",
                    "Share of World'sCO2 emissions": "Share_World_Percentage",
                }
            )
            self.save_to_s3(
                country_df_current,
                self.bucket_name,
                f"co2/country/{country}.csv"
            )

            country_df_sector = self.get_data_chart(
                country_co2_soup, 'global-co2-emissions-chart')

            if country_df_sector is not None and not country_df_sector.empty:
                self.save_to_s3(
                    country_df_sector,
                    self.bucket_name,
                    f"co2/country/{country}_sector.csv"
                )
