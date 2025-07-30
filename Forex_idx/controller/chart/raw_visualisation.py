import mplfinance as mpf
import pandas as pd
import os

def mapping_chart(dir: str, emiten: str, start_date: str):
    path_file = os.path.join(dir, emiten + '.csv')
    df = pd.read_csv(path_file)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d', errors='coerce')
    df = df[df['Date'] >= pd.to_datetime(start_date)]
    df.style.format({'Date': lambda t: t.strftime('%Y-%m-%d')})
    df['Open'] = df['OpenPrice']
    map = df[['Date','Open','High','Low','Close','Volume']]
    map.set_index('Date', inplace=True)
    style = mpf.make_mpf_style(
        marketcolors=mpf.make_marketcolors(up='green',
            down='red',
            wick={'up': 'green', 'down': 'red'},
            edge='i'
        )
    )
    mpf.plot(map, type='candle', style=style)

if __name__ == '__main__':
    mapping_chart(dir='data/raw', emiten='ANTM', start_date='20250101')