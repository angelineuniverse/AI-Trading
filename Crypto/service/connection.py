import logging
import hmac
import hashlib
import websockets
import base64
import os
import json
import asyncio
import time
from rich import print_json
from dotenv import load_dotenv
from helper.helper import load_binance_config
import requests
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class BinanceConnectionApi:
    def __init__(
                self,
                payload: dict = None,
                url: str = os.environ.get('URL_API',''), 
                reconnect_count: int = int(os.environ.get('RECONNECT_COUNT',0)), 
                reconnect_delay: int = int(os.environ.get('RECONNECT_DELAY',5)), 
                max_reconnect_attempts: int = int(os.environ.get('RECONNECT_MAX_ATTEMPTS',5)),
                interval: int = int(os.environ.get('INTERVAL',5)),
                folder_config: str = 'configuration',
                sub_url: str = ''
            ):
        self.reconnect_count = reconnect_count
        self.url = url
        self.interval = interval
        self.payload = payload
        self.folder_config = folder_config
        self.reconnect_delay = reconnect_delay
        self.sub_url = sub_url
        self.max_reconnect_attempts = max_reconnect_attempts
        self.logger = logging.getLogger("REQUEST")
        self.session = requests.Session()

    async def get(self):
        try:
            if self.payload:
                params = '&'.join([f'{param}={value}' for param, value in sorted(self.payload.items())])
                response = self.session.get(self.url + self.sub_url, params=params)
            else:
                response = self.session.get(self.url + self.sub_url)
            return response.json()
        except Exception as e:
            self.logger.error(f"Error occurred: {e}")
class BinanceConnectionSocket:
    def __init__(
                    self,
                    on_data: None,
                    extra_data: None,
                    payload: dict,
                    url: str = os.environ.get('URL_WS',''), 
                    reconnect_count: int = int(os.environ.get('RECONNECT_COUNT',0)), 
                    reconnect_delay: int = int(os.environ.get('RECONNECT_DELAY',5)), 
                    max_reconnect_attempts: int = int(os.environ.get('RECONNECT_MAX_ATTEMPTS',5)),
                    interval: int = int(os.environ.get('INTERVAL',5)),
                    folder_config: str = 'configuration',
                    use_log: bool = True,
                    disable_loop: bool = False
                ):
        self.reconnect_count = reconnect_count
        self.url = url
        self.use_log = use_log
        self.disable_loop = disable_loop
        self.on_data = on_data
        self.extra_data = extra_data
        self.interval = interval
        self.payload = payload
        self.folder_config = folder_config
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self.logger = logging.getLogger("REQUEST")

    async def generate_signature_ed(self, secret_key: str, payload: str):
        signature = base64.b64encode(secret_key.sign(payload.encode('ASCII')))
        return signature.decode('ASCII')

    async def generate_payload(self, params: dict):
        """
        Melakukan generate payload untuk 
        
        Args:
            params (dict): params berisi semua paramater dan timestamps yang di wajibkan pada saat memanggil request ( Baca Dokumentasi Bianance )
        
        Returns:
            params (str) : Hasil generate payload
        """
        payload = '&'.join([f'{param}={value}' for param, value in sorted(params.items())])
        return payload

    async def generate_signature_hmac(self, secret_key: str, payload: str):
        """
        Melakukan generate signature untuk API Key type HMAC
        
        Args:
            secret_key (str): Secret key yang dihasilkan oleh Binance
            payload (str): payload berisi semua paramater dan timestamps yang di wajibkan pada saat memanggil request ( Baca Dokumentasi Bianance )
        
        Returns:
            signature : Hasil generate signature
        """
        key_bytes = secret_key.encode('ascii')
        message_bytes = payload.encode('ascii')
        signature = hmac.new(key_bytes, message_bytes, hashlib.sha256)
        return signature.hexdigest()

    async def on_message(self, message):
        """Equivalent to on_message callback"""
        self.logger.info(f"Data received: {message}")

    async def on_info(self, message):
        """Equivalent to on_message callback"""
        self.logger.info(message)

    async def on_error(self, error):
        self.logger.error(f"Error occurred: {error}")

    async def on_ping(self):
        self.logger.debug("Ping received")

    async def need_signature(self):
        if not hasattr(self, 'payload') or not isinstance(self.payload, dict): # Jika pada class payload tidak didefinisi
            return False
        
        params = self.payload['params']
        if 'apiKey' in params and isinstance(params, dict):
            return True
        
        return False

    async def refresh_signature(self):
        current_timestamp = int(time.time() * 1000)
        
        configs = load_binance_config(config_folder=self.folder_config)
        params = {
            "apiKey" : configs['api_key'],
            "timestamp" : current_timestamp
        }
        params_signature = await self.generate_payload(params)
        signature = await self.generate_signature_ed(
            configs['private_key'],
            payload=params_signature
        )
        self.payload['params'].update({
            'timestamp': current_timestamp,
            'signature': signature
        })

    async def handle_reconnect_error(self, error):
        await self.on_error(error)
        self.reconnect_count += 1
        if self.reconnect_count < self.max_reconnect_attempts:
            await self.on_info(f"Reconnecting in {self.reconnect_delay} sec...")
            await asyncio.sleep(self.reconnect_delay)
        else:
            await self.on_error("Max reconnection attempts reached")

    async def periodic_refresh(self, ws):
        while True:
            try:
                await asyncio.sleep(self.interval)  # Jeda non-blocking
                await self.on_info(f"Interval in {self.interval} seconds")
                if await self.need_signature():
                    await self.refresh_signature()
                    await self.on_info("Signature refreshed !")
                await ws.send(json.dumps(self.payload))
            except Exception as e:
                await self.on_error(f"Refresh error: {e}")
                break

    async def call_request(self):
        while self.reconnect_count < self.max_reconnect_attempts:
            try:
                if await self.need_signature():
                    await self.refresh_signature()

                async with websockets.connect(self.url) as ws:
                    
                    await self.on_info("Connected to Binance WebSocket")
                    self.reconnect_count = 0
                    
                    # # 3. Kirim dan terima response
                    await ws.send(json.dumps(self.payload))
                    refresh_task = asyncio.create_task(self.periodic_refresh(ws))

                    try:
                        async for message in ws:
                            response_data = json.loads(message)
                            if self.use_log:
                                print_json(data=response_data)
                            if self.on_data:
                                self.on_data(response_data, self.extra_data)
                            if self.disable_loop:
                                return True
                    finally:
                        refresh_task.cancel()

            except Exception as e:
                await self.handle_reconnect_error(e)

    async def run(self):
        while True:
            try:
                success = await self.call_request()
                if not success:
                    self.reconnect_delay = min(self.reconnect_delay * 2, 60)
                    await self.logger.info("Waiting before next attempt...")
                    await asyncio.sleep(self.reconnect_delay)
                else:
                    break
            except KeyboardInterrupt:
                self.logger.info("Connection stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                await asyncio.sleep(5)  # Jeda sebelum mencoba lagi
