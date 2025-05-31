import os
import boto3

import pandas as pd
from abc import ABC, abstractmethod


class WorldStatistics(ABC):
    def __init__(self):
        self.minio_client = boto3.client(
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

    def convert_to_dataframe(rows):
        df = pd.DataFrame(rows[1:], columns=rows[0])
        return df

    def save_to_s3(self, df: pd.DataFrame, bucket: str, key: str):
        """Save DataFrame to MinIO"""
        csv_data = df.to_csv(index=False)
        self.minio_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_data
        )
        print(f"Saved to MinIO: {bucket}/{key}")
