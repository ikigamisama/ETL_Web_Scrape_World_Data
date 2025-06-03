import os
import boto3
import json
import re

import pandas as pd
from abc import ABC, abstractmethod
from dotenv import load_dotenv

load_dotenv()


class WorldStatistics(ABC):
    def __init__(self):
        self.s3_client = boto3.client(
            service_name="s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            region_name="us-east-1",
        )
        self.base_url = "https://www.worldometers.info"
        self.bucket_name = "etl-world-statistics"

    @abstractmethod
    async def scrape_and_save(self):
        """Main method to scrape and save data"""
        pass

    def save_to_s3(self, df: pd.DataFrame, bucket: str, key: str):
        """Save DataFrame to MinIO"""
        csv_data = df.to_csv(index=False)
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_data
        )
        print(f"Saved to MinIO: {bucket}/{key}")

    def save_csv(self, df: pd.DataFrame, path: str):
        df.to_csv(f"./data/{path}", index=False)
        print(f"Saved to Folder Path: {path}")

    def get_data_chart(self, soup, id, isFloat=False):
        div_tag = soup.find('div', id=id)
        if div_tag is None:
            return None

        script_tag = div_tag.find_next_sibling('script')
        if script_tag is None or script_tag.string is None:
            return None

        match = re.search(
            r'const data\s*=\s*(\[\{.*?\}\]);', script_tag.string, re.DOTALL)

        if match:
            data_json_str = match.group(1)
            co2_sector_data = json.loads(data_json_str)

            if isFloat:
                pd.set_option('display.float_format', '{:.0f}'.format)
            else:
                pd.reset_option('display.float_format')

            return pd.DataFrame(co2_sector_data)

        return None

    def convert_to_dataframe(self, rows):
        df = pd.DataFrame(rows[1:], columns=rows[0])
        return df

    def get_data_from_table(self, soup, columns, table_index=0):
        table = soup.find_all('table', class_="datatable-table")[table_index]
        rows = []

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            row = [cell.get_text(strip=True) for cell in cells]
            rows.append(row)

        df = self.convert_to_dataframe(rows)
        df = df.rename(columns=columns)
        return df
