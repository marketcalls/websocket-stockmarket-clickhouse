import asyncio
import websockets
import json
import aiohttp
from datetime import datetime
import clickhouse_connect
from smartWebSocketV2 import SmartWebSocketV2
import os
from dotenv import load_dotenv
import backoff

# Load environment variables
load_dotenv()

# AngelOne credentials and configuration
client_id = os.getenv('ANGEL_CLIENT_ID')
client_pin = os.getenv('ANGEL_CLIENT_PIN')
totp_code = os.getenv('ANGEL_TOTP_CODE')
api_key = os.getenv('ANGEL_API_KEY')
client_local_ip = os.getenv('ANGEL_CLIENT_LOCAL_IP')
client_public_ip = os.getenv('ANGEL_CLIENT_PUBLIC_IP')
mac_address = os.getenv('ANGEL_MAC_ADDRESS')
state_var = os.getenv('ANGEL_STATE_VARIABLE')

# Global variables for tokens
AUTH_TOKEN = None
FEED_TOKEN = None

# ClickHouse configuration
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

# Initialize ClickHouse client
client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, 
                                       username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)

def create_clickhouse_table():
    query = '''
    CREATE TABLE IF NOT EXISTS angelone_market_data (
        token String,
        timestamp DateTime64(3),
        last_traded_price Float64,
        open_price Float64,
        high_price Float64,
        low_price Float64,
        close_price Float64,
        volume Float64
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    '''
    client.command(query)
    print("Connected to ClickHouse. Server version:", client.server_version)

def store_data_in_clickhouse(data):
    try:
        values = [
            (data['token'],
             datetime.now(),
             data['last_traded_price'],
             data['open_price_of_the_day'],
             data['high_price_of_the_day'],
             data['low_price_of_the_day'],
             data['closed_price'],
             data['volume_trade_for_the_day'])
        ]
        client.insert('angelone_market_data', values)
    except Exception as e:
        print(f"Error storing data in ClickHouse: {e}")

async def generate_tokens():
    global AUTH_TOKEN, FEED_TOKEN
    
    url = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"
    
    payload = json.dumps({
        "clientcode": client_id,
        "password": client_pin,
        "totp": totp_code,
        "state": state_var
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-UserType': 'USER',
        'X-SourceID': 'WEB',
        'X-ClientLocalIP': client_local_ip,
        'X-ClientPublicIP': client_public_ip,
        'X-MACAddress': mac_address,
        'X-PrivateKey': api_key
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                AUTH_TOKEN = data['data']['jwtToken']
                FEED_TOKEN = data['data']['feedToken']
                print("Tokens generated successfully.")
            else:
                print(f"Failed to generate tokens. Status: {response.status}")
                print(await response.text())
                raise Exception("Token generation failed")

@backoff.on_exception(backoff.expo, Exception, max_tries=10)
async def connect_with_retry():
    return await connect_websocket()

async def connect_websocket():
    if not AUTH_TOKEN or not FEED_TOKEN:
        print("Tokens are not set. Make sure to generate tokens first.")
        return

    uri = "wss://smartapisocket.angelone.in/smart-stream"
    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "x-api-key": api_key,
        "x-client-code": client_id,
        "x-feed-token": FEED_TOKEN
    }

    while True:
        try:
            async with websockets.connect(uri, extra_headers=headers, 
                                          ping_interval=20, ping_timeout=10) as websocket:
                print("WebSocket connected")
                await subscribe_to_quotes(websocket)
                
                ws_parser = SmartWebSocketV2(AUTH_TOKEN, api_key, client_id, FEED_TOKEN)
                
                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=30)
                        if isinstance(message, bytes):
                            parsed_message = ws_parser._parse_binary_data(message)
                            if parsed_message:
                                process_and_store_data(parsed_message)
                    except asyncio.TimeoutError:
                        print("No data received for 30 seconds, sending ping...")
                        pong_waiter = await websocket.ping()
                        await asyncio.wait_for(pong_waiter, timeout=10)
                    except websockets.exceptions.ConnectionClosed:
                        print("WebSocket connection closed, reconnecting...")
                        break
        except Exception as e:
            print(f"Error in WebSocket connection: {e}")
        
        print("Attempting to reconnect in 5 seconds...")
        await asyncio.sleep(5)

def process_and_store_data(data):
    try:
        adjusted_data = {
            'token': str(data['token']),
            'last_traded_price': float(data['last_traded_price']) / 100,
            'open_price_of_the_day': float(data.get('open_price_of_the_day', 0)) / 100,
            'high_price_of_the_day': float(data.get('high_price_of_the_day', 0)) / 100,
            'low_price_of_the_day': float(data.get('low_price_of_the_day', 0)) / 100,
            'closed_price': float(data.get('closed_price', 0)) / 100,
            'volume_trade_for_the_day': float(data.get('volume_trade_for_the_day', 0))
        }
        
        store_data_in_clickhouse(adjusted_data)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        minute = datetime.now().strftime("%H:%M")
        
        output = f"[{current_time}] {adjusted_data['token']} - minute: {minute} | "
        output += f"open: {adjusted_data['open_price_of_the_day']:.1f} | "
        output += f"close: {adjusted_data['last_traded_price']:.1f} | "
        output += f"high: {adjusted_data['high_price_of_the_day']:.1f} | "
        output += f"low: {adjusted_data['low_price_of_the_day']:.1f} | "
        output += f"volume: {adjusted_data['volume_trade_for_the_day']:.3f}"
        
        print(output, flush=True)
    except Exception as e:
        print(f"Error in process_and_store_data: {e}")
        print(f"Error occurred with data: {data}")

async def subscribe_to_quotes(websocket):
    subscribe_message = json.dumps({
        "correlationID": "ws_test",
        "action": 1,
        "params": {
            "mode": 2,
            "tokenList": [{"exchangeType": 1, "tokens": ["2885"]}]  # RELIANCE token
        }
    })
    await websocket.send(subscribe_message)
    print("Subscribed to RELIANCE quotes")

async def main():
    create_clickhouse_table()
    while True:
        try:
            await generate_tokens()
            await connect_with_retry()
        except Exception as e:
            print(f"An error occurred: {e}")
        print("Restarting the entire process in 10 seconds...")
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())