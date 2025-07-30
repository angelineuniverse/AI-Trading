# from config import BinanceConnection, load_binance_config
from helper.helper import load_binance_config
from service.connection import BinanceConnection
import logging
import asyncio
import uuid
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ACCOUNT_DETAIL")

def running():
    try:
        configs = load_binance_config(config_folder='configuration')
        payload = {
            "id": str(uuid.uuid4()),
            "method": "account.status",
            "params": {
                "apiKey": configs['api_key'],
            }
        }
        config = BinanceConnection(
            payload=payload,
            interval=4
        )
        asyncio.run(config.run())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == '__main__':
    running()