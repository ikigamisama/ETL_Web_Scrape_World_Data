import re
import pandas as pd

from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class Water(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        water_world_soup = await scrape_url("https://www.worldometers.info/water/", ".datatable-table")
        country_water_df = self._scrape_water_country_table(water_world_soup)

        country_water_final_df = await self._process_water_country_data(
            country_water_df)

        self.save_to_s3(country_water_final_df,
                        self.bucket_name, "water/country_water.csv")

    def _scrape_water_country_table(self, soup):
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
                    if j == 0 and a_tag and a_tag.get("href"):
                        row.append(text)
                        row.append(self.base_url + a_tag["href"])
                    else:
                        row.append(text)

            rows.append(row)

        df = pd.DataFrame(rows[1:], columns=rows[0])
        df = df.drop(columns={'Population'}).rename(columns={
            'YearlyWater Used(m³, thousand of litres)': "Yearly_Water_Used",
            'Daily Water UsedPer Capita (litres)': "Daily_Water_Used_Per_Capita"
        })
        df.loc[:, "Yearly_Water_Used"] = df["Yearly_Water_Used"].str.split(
            "Year").str[0]

        return df

    async def _process_water_country_data(self, df):
        df = df.copy()
        df["Water_Precipitation_Depth"] = None
        df["Water_Precipitation_Volume"] = None
        df["Renewable_Water_Resource"] = None
        df["Water_Resource_Per_Capita"] = None
        df["Water_Dependency"] = None

        for i, row in df.iterrows():
            link = row['Link']
            Country = row['Country']
            country_water_soup = await scrape_url(link, "#see-also")

            def safe_get_text(data_list, index):
                try:
                    return data_list[index].get_text(strip=True)
                except (IndexError, AttributeError):
                    return None

            def parse_water_volume(text):
                if not text or not isinstance(text, str):
                    return None

                match = re.match(
                    r"([\d.,]+)\s*(billion|million)?", text.lower().replace(" ", ""))
                if not match:
                    return None

                number_str, unit = match.groups()
                number = float(number_str.replace(',', ''))

                if unit == "billion":
                    return int(number * 1_000_000_000)
                elif unit == "million":
                    return int(number * 1_000_000)
                else:
                    return int(number)

            try:
                water_precipitation_data = country_water_soup.find('h2', id="water-precipitation") \
                    .find_next_sibling('div') \
                    .find_all('div', attrs={'class': ['text-2xl', 'font-bold', 'mb-1.5']})
            except AttributeError:
                water_precipitation_data = []

            try:
                water_resource_data = country_water_soup.find('h2', id="water-resources") \
                    .find_next_sibling('div') \
                    .find_all('div', attrs={'class': ['text-2xl', 'font-bold', 'mb-1.5']})
            except AttributeError:
                water_resource_data = []

            # Assign scraped values to DataFrame
            df.at[i, "Water_Precipitation_Depth_MM"] = safe_get_text(
                water_precipitation_data, 0)
            df.at[i, "Water_Precipitation_Volume"] = parse_water_volume(
                safe_get_text(water_precipitation_data, 1))
            df.at[i, "Renewable_Water_Resource"] = parse_water_volume(
                safe_get_text(water_resource_data, 0))
            df.at[i, "Water_Resource_Per_Capita_Area"] = safe_get_text(
                water_resource_data, 1)
            df.at[i, "Water_Dependency_Percent"] = safe_get_text(
                water_resource_data, 2)

            water_resource_country = self.get_data_chart(
                country_water_soup, "water-resources-chart")
            water_consumption_chart = self.get_data_chart(
                country_water_soup, "water-consumption-chart")

            water_country_df = pd.merge(
                water_resource_country, water_consumption_chart, how="right", on="year")

            self.save_to_s3(water_country_df, self.bucket_name,
                            f"water/country/{Country}.csv")

        df["Water_Precipitation_Depth_MM"] = df["Water_Precipitation_Depth_MM"].str.split(
            ' ').str[0]
        df["Water_Dependency_Percent"] = df["Water_Dependency_Percent"].str.replace(
            '%', "")
        df["Water_Resource_Per_Capita_Area"] = df["Water_Resource_Per_Capita_Area"].str.split(
            'm³').str[0]
        return df
