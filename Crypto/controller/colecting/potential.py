from service.connection import BinanceConnectionApi
from helper.helper import write_coin_potential
import pandas as pd
import json
import asyncio

async def get_potential_coin(dir_coin: str):
    df = pd.read_csv(dir_coin)
    tickerRequest = BinanceConnectionApi(
        sub_url="/ticker/24hr",
        payload={
            "symbols": json.dumps(df['symbol'].tolist(), separators=(',', ':')).replace('[', '%5B').replace(']', '%5D').replace('"', '%22') # parameter hasil convert
        }
    )
    ticker = await tickerRequest.get()
    return write_coin_potential('log/data', 'resource', ticker)

if __name__ == '__main__':
    asyncio.run(get_potential_coin(dir_coin='log/data/raw.csv'))
    