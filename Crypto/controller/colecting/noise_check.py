from helper.helper import check_noise_data
import logging
import os
import pandas as pd
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_noise(dir: str):
    os.makedirs(dir, exist_ok=True)
    files = [f for f in os.listdir(dir) if f.endswith('.csv')]

    for file in files:
        filename_only = os.path.splitext(file)[0]
        check_noise_data(dir='log/data/raw', filename=filename_only)

if __name__ == '__main__':
    try:
        check_noise(dir='log/data/raw')
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")