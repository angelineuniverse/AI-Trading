from service.connection import BinanceConnectionApi
from helper.helper import write_history_klines
import pandas as pd
import time
import logging
import asyncio
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def history_klines_coin(year, filecoin: str, delay: float):
    today = pd.Timestamp.now()
    end_time = today - pd.DateOffset(months=2)
    start_time = end_time - pd.DateOffset(years=year)
    start_timestamps = int(start_time.timestamp() * 1000)
    end_timestamps = int(end_time.timestamp() * 1000)

    list_coin = pd.read_csv('log/'+filecoin)
    for index, row in list_coin.iterrows():
        if not delay:
            break
        while start_timestamps < end_timestamps:
            if not delay:
                break
            time.sleep(delay)
            config = BinanceConnectionApi(
                sub_url='/klines',
                payload={
                    "symbol" : row['symbol'],
                    "interval" : '1m',
                    "startTime": start_timestamps,
                    "endTime": end_timestamps,
                    "limit": 1000
                }
            )
            response = await config.get()
            if not response:
                logging.error("⛔ Response tidak diberikan oleh Binance !")
                return
            await write_history_klines(dir='log/data/raw', filename=row['symbol'], data=response)
            next_timestamps = pd.to_datetime(start_timestamps, unit='ms') + pd.Timedelta(hours=5)
            next_timestamps = int(next_timestamps.timestamp() * 1000)
            start_timestamps = next_timestamps
            logging.info(f"start time : {pd.to_datetime(start_timestamps, unit='ms')}, end time : {end_time}")

if __name__ == '__main__':
    try:
        asyncio.run(history_klines_coin(filecoin='resource.csv', year=1, delay=5.0))
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        #2025-01-22 18:40:20