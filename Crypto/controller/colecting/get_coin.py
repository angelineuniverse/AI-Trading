from service.connection import BinanceConnectionApi
from helper.helper import write_coin
import pandas as pd
import time
import asyncio

async def get_coin(year: int  = 2, limit: int = 400, delay: float = 15.0, quoteAsset: str = 'USDT'):
    timestamp_n_years_ago = int((pd.Timestamp.utcnow() - pd.Timedelta(days=365*year)).timestamp() * 1000)
    exchageRequest = BinanceConnectionApi(
        sub_url="/exchangeInfo"
    )
    exchangeInfo = await exchageRequest.get()
    data_coin = []
    _index = 0
    for item in exchangeInfo["symbols"]:
        if (
            item["status"] == "TRADING" and
            item["quoteAsset"] == quoteAsset and
            item["isSpotTradingAllowed"] and
            (
                "SPOT" in item.get("permissions", []) or
                any("SPOT" in s for s in item.get("permissionSets", []))
            )
        ):
            _index = _index + 1
            if len(data_coin) >= limit: # sudah limit maka stop loop
                break

            time.sleep(delay)
            klinesRequest = BinanceConnectionApi(
                sub_url="/klines",
                payload={
                    "symbol": item['symbol'],
                    "interval": "1M",
                    "limit": 1,
                    "startTime": timestamp_n_years_ago
                }
            )
            klines = await klinesRequest.get()
            if isinstance(klines, list) and klines: # get list
                years_coin_listing = pd.to_datetime(int(klines[0][0]), unit="ms") # tahun coin listing
                now_years = pd.Timestamp.utcnow().tz_localize(None) # tahun sekarang
                if (now_years.year - years_coin_listing.year) == year: # tahun coin - tahun sekarang = year(parameter)
                    data_coin.append(item['symbol']) # add coin symbol to Array
                    print(F"COIN 2 YEARS AGO, [{_index}]: {item['symbol']}")

    return write_coin('log/data', 'raw', data_coin)

if __name__ == '__main__':
    try:
        asyncio.run(get_coin(delay=5.0))
    except KeyboardInterrupt:
        print("Berhenti memanggil API.")