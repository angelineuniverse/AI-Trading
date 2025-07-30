import asyncio
import websockets
import logging
import json
import hmac
import hashlib
import time
import uuid
from rich import print_json
# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BinanceWS")

class BinanceWebSocket:
    def __init__(self):
        self.url = "wss://ws-api.binance.com:443/ws-api/v3"
        self.reconnect_delay = 5
        self.max_reconnect_attempts = 5
        self.reconnect_count = 0
        self.api_secret = ""
        self.api_key = ""

    async def on_message(self, message):
        logger.info(f"Data received: {message}")

    async def on_error(self, error):
        logger.error(f"Error occurred: {error}")

    async def on_ping(self):
        logger.debug("Ping received")

    async def generate_signature(self,secret_key: str, payload: str):
        key_bytes = secret_key.encode('ascii')
        message_bytes = payload.encode('ascii')
        signature = hmac.new(key_bytes, message_bytes, hashlib.sha256)
        return signature.hexdigest()

    async def get_account_info(self):
        while self.reconnect_count < self.max_reconnect_attempts:
            try:
                async with websockets.connect(self.url) as ws:
                    logger.info("Connected to Binance WebSocket")
                    self.reconnect_count = 0
                    # 1. Generate signature
                    timestamp = int(time.time() * 1000)
                    secret = ''
                    api = ''
                    params = f"apiKey={api}&timestamp={timestamp}"  # HANYA timestamp!
                    signature = await self.generate_signature(secret,params)

                    # 2. Buat request payload
                    request = {
                        "id": str(uuid.uuid4()),  # UUID unik
                        "method": "session.logon",  # Ganti dengan method yang diinginkan
                        "params": {
                            "apiKey": api,
                            "signature": signature,
                            "timestamp": timestamp  # Sama dengan yang di-sign
                        }
                    }

                    # 3. Kirim dan terima response
                    await ws.send(json.dumps(request))
                    response = await ws.recv()
                    print_json(data=json.loads(response))
            except Exception as e:
                await self.on_error(f"Connection failed: {e}")
                self.reconnect_count += 1
                if self.reconnect_count < self.max_reconnect_attempts:
                    logger.info(f"Reconnecting in {self.reconnect_delay} sec...")
                    await asyncio.sleep(self.reconnect_delay)
                else:
                    logger.error("Max reconnection attempts reached")
                    break

    async def request_account_balance(ws, signature ,api_key, timestamp):
        request = {
            "id": str(uuid.uuid4()),
            "method": "session.logon",
            "params": {
                "apiKey": api_key,
                "signature": signature,
                "timestamp": timestamp
            }
        }
        await ws.send(json.dumps(request))

    async def run(self):
        while True:
            await self.get_account_info()
            self.reconnect_delay = min(self.reconnect_delay * 2, 60)
            await asyncio.sleep(self.reconnect_delay)

if __name__ == "__main__":
    ws_client = BinanceWebSocket()
    
    try:
        asyncio.run(ws_client.run())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")