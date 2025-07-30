from pathlib import Path
import configparser
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from dotenv import load_dotenv
import logging
import pandas as pd
import os
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_binance_config(config_folder: str, config_file: str = 'config.ini') -> dict:
    """
    Memuat konfigurasi API Binance dengan path relatif terhadap root project.
    
    Args:
        config_folder (str): Folder yang berisikan file config.ini
    
    Returns:
        dict: Dictionary berisi config dengan path yang sudah di-resolve
    """
    # Temukan root project
    project_root = Path().absolute()
    
    # Resolve path config
    config_abs_path = (project_root / config_folder / config_file).resolve()
    if not config_abs_path.exists():
        raise FileNotFoundError(f"file config '{config_abs_path}' tidak ditemukan")
    
    # Baca config
    config = configparser.ConfigParser()
    config.read(config_abs_path)
    
    api_key = config['keys']['API_KEY']
    private_key_filename = config['keys']['PRIVATE_KEY']
    private_key_password = config['keys']['PRIVATE_KEY_PASSWORD'] or None
    
    
    private_key_abs_path = (project_root / private_key_filename).resolve()
    if not private_key_abs_path.exists():
        raise FileNotFoundError(f"File private key '{private_key_abs_path}' tidak ditemukan didalam variable config.ini")
    
    # Load private key
    with open(private_key_abs_path, 'rb') as key_file:
        private_key_data = key_file.read()
    
    try:
        password = (private_key_password.encode() 
                if private_key_password else None)
        private_key = load_pem_private_key(
            private_key_data,
            password=password,
            backend=default_backend()
        )
        
        if not isinstance(private_key, Ed25519PrivateKey):
            raise ValueError("Private key bukan tipe Ed25519")
            
    except Exception as e:
        raise ValueError(f"Gagal memuat private key: {str(e)}")
    
    return {
        'api_key': api_key,
        'private_key': private_key,
        'private_key_password': private_key_password,
        'private_key_path': str(private_key_abs_path),
        'project_root': str(project_root)  # Untuk referensi
    }

def write_coin(dir: str, filename: str, data: dict):
    if not data:
        return
    
    rows = []
    for item in data:
        rows.append({
            "symbol": item,
        })

    if not rows:
        return
    
    df = pd.DataFrame(rows)
    df.to_csv(f"{dir}/{filename}.csv", index=False) # get 20 COIN
    logging.info("Create Coin !")

def write_coin_potential(dir: str, filename: str, data: dict):
    if not data:
        return
    
    rows = []
    for item in data:
        symbol = item['symbol']
        last_price = float(item['lastPrice'])
        volume = float(item['volume'])
        percentage = float(item['priceChangePercent'])

        if last_price == 0 or volume == 0: # if coin not exist on market
            continue

        rows.append({
            "symbol": symbol,
            "priceChangePercent": percentage,
            "lastPrice": last_price,
            "volume": volume
        })

    if not rows:
        return
    
    df = pd.DataFrame(rows)
    sorted = df.sort_values(by='priceChangePercent', ascending=False) # sort by top percent
    sorted.head(20).to_csv(f"{dir}/{filename}.csv", index=False) # get 20 COIN
    logging.info("Create Potential !")

def write_history_klines(dir: str, filename: str, data: dict):
    if not data:
        return
    
    rows = []
    for item in data:
        rows.append({
            "open_time": item[0],
            "open": item[1],
            "high": item[2],
            "low": item[3],
            "close": item[4],
            "volume": item[5],
            "close_time": item[6],
            "quote_asset_volume": item[7],
            "num_trades": item[8],
            "taker_buy_base_volume": item[9],
            "taker_buy_quote_volume": item[10],
        })

    if not rows:
        return
    
    os.makedirs(dir, exist_ok=True)
    filenames = filename + '.csv'
    file_path = os.path.join(dir, filenames)
    new_data = pd.DataFrame(rows)

    if os.path.exists(file_path):
        logging.info(f"[📄] File ditemukan: {filenames}")
        try:
            existing = pd.read_csv(file_path)
            new_data = new_data[existing.columns.tolist()] # samakan column dengan data yang sudah ada
            new_df = new_data[~new_data['open_time'].isin(existing['open_time'])]
            if not new_df.empty:
                combine_data = pd.concat([existing, new_data], ignore_index=True)
                combine_data.drop_duplicates(subset='open_time', keep='last', inplace=True) # duplicate when open_match match to another, and get lasted
                combine_data.to_csv(file_path, index=False)
                logging.info(f"[✅] Data berhasil ditambahkan dengan filter duplikat")
            else:
                logging.info(f"[ℹ️] Tidak ada data baru, dilewati")
        except Exception as e:
            logging.error(f"[⛔] Gagal menambahkan data ke file: {e}")
    else:
        logging.info(f"[🆕] File belum ada, membuat baru: {filenames}")
        try:
            df = pd.DataFrame(rows)
            df.to_csv(file_path, index=False)
            print(f"[✅] File baru berhasil dibuat.")
        except Exception as e:
            print(f"[⛔] Gagal membuat file: {e}")

def check_noise_data(dir: str, filename: str):
    os.makedirs(dir, exist_ok=True)
    filenames = filename + '.csv'
    file_path = os.path.join(dir, filenames)

    if os.path.exists(file_path):
        logging.info(f"[📄] File ditemukan: {filenames}")
    
        data = pd.read_csv(file_path)
        data['open_time'] = pd.to_datetime(data['open_time'], unit='ms')
        data = data.sort_values(by='open_time').reset_index(drop=True)

        # Tambahkan previous_time sebelum membuat subset
        data['previous_time'] = data['open_time'].shift(1)

        # Hitung selisih antar waktu
        data['diff'] = data['open_time'].diff().dt.total_seconds()

        # Ambil data yang punya gap > 60 detik
        missing_info = data[data['diff'] > 60][['previous_time', 'open_time', 'diff']]
        missing_info['candles_missing'] = (missing_info['diff'] // 60 - 1).astype(int)
        logging.info(f"[🔍][{filename}] GAP waktu antar data : {data['open_time'].iloc[1] - data['open_time'].iloc[0]}")
        logging.info(f"[🔍][{filename}] data yang hilang: {missing_info.head(10)}")
        logging.info(f"[⚠️][{filename}] Total data lebih dari 60 detik: {len(missing_info)}")