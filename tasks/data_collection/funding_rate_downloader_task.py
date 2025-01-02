import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pandas as pd
import requests
from dotenv import load_dotenv

from core.services.timescale_client import TimescaleClient
from core.task_base import BaseTask

logging.basicConfig(level=logging.INFO)
load_dotenv()


class FundingRateDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.quote_asset = config.get("quote_asset", "USDT")
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(
            f"{now} - Starting funding rate downloader for {self.connector_name}"
        )

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        trading_pairs = self.config.get("trading_pairs", ["BTC-USDT"])
        
        for trading_pair in trading_pairs:
            try:
                symbol = trading_pair.replace("-", "")
                table_name = f"funding_rates_{self.connector_name.lower()}"
                
                last_timestamp = await timescale_client.get_last_timestamp(
                    table_name=table_name,
                    trading_pair=trading_pair
                )
                
                params = {
                    "exchange": self.connector_name.replace("_perpetual", ""),
                    "symbol": symbol,
                    "interval": "1h"
                }
                if last_timestamp:
                    # Convert to milliseconds for API
                    params["start"] = int(last_timestamp.timestamp() * 1000)

                url = f"https://open-api-v3.coinglass.com/api/futures/fundingRate/ohlc-history"
                headers = {
                    "accept": "application/json",
                    "CG-API-KEY": self.api_key
                }

                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching data for {trading_pair}: {response.text}")
                    continue

                data = response.json()
                if not data.get("data"):
                    logging.info(f"No funding rate data for {trading_pair}")
                    continue

                funding_rates = pd.DataFrame(data["data"])
                funding_rates["timestamp"] = pd.to_datetime(funding_rates["timestamp"], unit="ms")
                funding_rates["created_at"] = datetime.now(timezone.utc)

                await timescale_client.create_funding_rates_table(table_name)

                funding_data = funding_rates[
                    ["open", "high", "low", "close", "timestamp", "created_at"]
                ].values.tolist()

                await timescale_client.append_funding_rates(
                    table_name=table_name,
                    funding_rates=funding_data
                )

                # Delete old data
                cutoff_timestamp = (datetime.now() - timedelta(days=self.days_data_retention)).timestamp()
                await timescale_client.delete_funding_rates(
                    connector_name=self.connector_name,
                    trading_pair=trading_pair,
                    timestamp=cutoff_timestamp
                )

                logging.info(f"{now} - Inserted funding rates for {trading_pair}")
                await asyncio.sleep(1)  # Rate limiting

            except Exception as e:
                logging.exception(f"{now} - Error processing {trading_pair}: {e}")
                continue

        await timescale_client.close()

    @staticmethod
    def now():
        return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f UTC')


if __name__ == "__main__":
    config = {
        "connector_name": "binance_perpetual",
        "quote_asset": "USDT",
        "days_data_retention": 30,
        "coinglass_api_key": "your-api-key-here",
        "trading_pairs": ["BTC-USDT", "ETH-USDT"],
        "timescale_config": {
            "host": "localhost",
            "port": 5432,
            "user": "admin",
            "password": "admin",
            "database": "timescaledb"
        }
    }
    
    task = FundingRateDownloaderTask(
        "Funding Rate Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute()) 