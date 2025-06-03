from functions.world_statistics import WorldStatistics
from functions.WebScraper import scrape_url


class Energy(WorldStatistics):
    def __init__(self):
        super().__init__()

    async def scrape_and_save(self):
        energy_world = await scrape_url("https://www.worldometers.info/energy/", ".datatable-table")

        world_energy_df = self.get_data_from_table(energy_world, {
            'EnergyConsumption(BTU)': "Energy_Consumption_BTU",
            'WorldShare': 'World_Share',
            'Per capitaYearly BTU': "Yearly_Capita_BTU"
        })

        self.save_to_s3(world_energy_df, self.bucket_name,
                        "energy/world_energy.csv")
