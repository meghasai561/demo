import datetime
import time
import json
import logging
import pyotp
import requests
from datetime import timedelta
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Configure logging to file
logging.basicConfig(
    filename='trading_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Configuration - Replace with your actual credentials
API_KEY = "UqpiUvRZ"
CLIENT_ID = "S387905"
PASSWORD = "5612"
TOTP_SECRET = "PTHQZWA2P75ES2ENO3UILLSAJY"  # Your TOTP secret from QR code
IS_PAPER = True  # Set to False for live trading
IS_PAPER = True  # Set to False for live trading

# Tokens
BANKNIFTY_TOKEN = "26009"  # BankNifty index token
EXCHANGE = "NSE"

# Trading Parameters
ENTRY_THRESHOLD = 35  # Points above/below 30-min high/low to enter
TARGET = 10  # Profit target in points
STOP_LOSS = 35  # Stop loss in points
LOT_SIZE = 30  # BankNifty options lot size
STRIKE_INTERVAL = 100  # BankNifty strike interval
NO_ENTRY_AFTER = datetime.time(15, 5)  # No new entries after 3:05 PM
EXIT_TIME = datetime.time(15, 15)  # Exit positions by 3:15 PM

# Global variables
smart_api = None
web_socket = None
first_candle_high = None
first_candle_low = None
position = None  # {'type': 'PUT' or 'CALL', 'strike': , 'instrument_token': , 'entry_price': , 'entry_time': }
candle_data = []  # List of ticks for current candle
current_expiry = None
call_traded_today = False  # Track if CALL leg traded today
put_traded_today = False  # Track if PUT leg traded today
instruments = []  # List of NFO instruments
start_time = None  # Time when WebSocket opens (market start)
last_candle_time = None  # Last time a candle was processed
first_candle_duration = timedelta(minutes=30)
subsequent_candle_duration = timedelta(minutes=3)
market_open = datetime.time(9, 15)
market_close = datetime.time(15, 30)

def authenticate():
    global smart_api, current_expiry, instruments
    smart_api = SmartConnect(api_key=API_KEY)
    # Generate TOTP
    totp = pyotp.TOTP(TOTP_SECRET)
    TOTP_PIN = totp.now()
    # Generate session
    data = smart_api.generateSession(CLIENT_ID, PASSWORD, TOTP_PIN)
    if data['status']:
        logging.info("Authentication successful")
        # Fetch instrument list
        try:
            response = requests.get("https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json")
            if response.status_code == 200:
                all_instruments = response.json()
                instruments = [i for i in all_instruments if i.get('exch_seg') == 'NFO']
                logging.info(f"Fetched {len(instruments)} NFO instruments")
                # Get current expiry for BankNifty
                banknifty_options = [i for i in instruments if i.get('name') == 'BANKNIFTY']
                if banknifty_options:
                    expiries = []
                    for opt in banknifty_options:
                        try:
                            exp_date = datetime.datetime.strptime(opt['expiry'], '%d%b%Y').date()
                            expiries.append(exp_date)
                        except:
                            pass
                    if expiries:
                        current_expiry = min(expiries)
                        logging.info(f"Current BankNifty expiry: {current_expiry}")
            else:
                logging.error("Failed to fetch instrument list")
        except Exception as e:
            logging.error(f"Error fetching instruments: {e}")
        return True
    else:
        logging.error(f"Authentication failed: {data.get('message', 'Unknown error')}")
        return False

def get_option_instrument(strike, option_type):
    global current_expiry, instruments
    if not current_expiry or not instruments:
        return None
    expiry_str = current_expiry.strftime('%d%b%Y').upper()
    symbol = f"BANKNIFTY{expiry_str}{strike}{option_type[0]}E"  # PE or CE
    for inst in instruments:
        if inst['symbol'] == symbol:
            return {'token': inst['token'], 'symbol': symbol}
    return None

def place_order(instrument_token, transaction_type, quantity, symbol):
    orderparams = {
        "variety": "NORMAL",
        "tradingsymbol": symbol,
        "symboltoken": instrument_token,
        "transactiontype": transaction_type,  # "BUY" or "SELL"
        "exchange": "NFO",
        "ordertype": "MARKET",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "quantity": quantity
    }
    response = smart_api.placeOrder(orderparams)
    if response['status']:
        logging.info(f"Order placed successfully: {transaction_type} {quantity} {symbol} (Token: {instrument_token})")
    else:
        logging.error(f"Error placing order: {response.get('message', 'Unknown error')} for {transaction_type} {quantity} {symbol}")
    return response

def get_historical_candle(token, from_time, to_time, interval='30minute'):
    # Fetch historical candle data
    try:
        data = smart_api.getCandleData("NSE", token, interval, from_time.strftime('%Y-%m-%d %H:%M'), to_time.strftime('%Y-%m-%d %H:%M'))
        if data and 'data' in data and data['data']:
            candle = data['data'][0]  # Assuming one candle
            return {
                'high': float(candle[2]),
                'low': float(candle[3])
            }
    except Exception as e:
        logging.error(f"Error fetching historical candle: {e}")
    return None

def on_message(ws, message):
    global first_candle_high, first_candle_low, position, candle_data, last_candle_time
    try:
        data = json.loads(message)
        if 'data' not in data:
            return
        
        for tick in data['data']:
            timestamp = datetime.datetime.fromtimestamp(tick['timestamp'])
            ltp = tick['ltp']
            # For simplicity, use ltp as high/low/close for tick
            high = ltp
            low = ltp
            close = ltp
            candle_data.append({'high': high, 'low': low, 'close': close, 'time': timestamp})
        
        current_time = datetime.datetime.now()
        
        # Determine candle duration
        if first_candle_high is None:
            candle_duration = first_candle_duration
        else:
            candle_duration = subsequent_candle_duration
        
        # Check if enough time has passed for a new candle
        if current_time >= last_candle_time + candle_duration and candle_data:
            # Calculate candle high, low, close
            prices = [d['close'] for d in candle_data]  # Use close for simplicity, or calculate properly
            candle_high = max([d['high'] for d in candle_data])
            candle_low = min([d['low'] for d in candle_data])
            candle_close = candle_data[-1]['close']
            candle_time = candle_data[-1]['time']
            
            if first_candle_high is None:
                # Set first 30-min candle
                first_candle_high = candle_high
                first_candle_low = candle_low
                logging.info(f"First candle high: {first_candle_high}, low: {first_candle_low}")
            else:
                # Process subsequent 3-min candles
                process_candle(candle_high, candle_low, candle_close, candle_time)
            
            # Reset for next candle
            candle_data = []
            last_candle_time = current_time
    
    except Exception as e:
        logging.error(f"Error in on_message: {e}")

def process_candle(high, low, close, time):
    global first_candle_high, first_candle_low, position, call_traded_today, put_traded_today
    if time.time() < market_open:
        return
    if first_candle_high is None:
        # First 30 min candle
        first_candle_high = high
        first_candle_low = low
        logging.info(f"First candle high: {first_candle_high}, low: {first_candle_low}")
        return

    if position is None and time.time() <= NO_ENTRY_AFTER:
        # Check entry conditions for 3 min candles
        if close > first_candle_high + ENTRY_THRESHOLD and not call_traded_today:
            # Buy CALL
            strike = round(close / STRIKE_INTERVAL) * STRIKE_INTERVAL
            inst = get_option_instrument(strike, 'CALL')
            if inst:
                response = place_order(inst['token'], "BUY", LOT_SIZE, inst['symbol'])
                if response['status']:
                    entry_price = get_ltp(inst['token'])
                    position = {
                        'type': 'CALL',
                        'strike': strike,
                        'instrument_token': inst['token'],
                        'symbol': inst['symbol'],
                        'entry_price': entry_price,
                        'entry_time': time
                    }
                    call_traded_today = True
                    logging.info(f"Bought CALL {strike} at {entry_price}")
        elif close < first_candle_low - ENTRY_THRESHOLD and not put_traded_today:
            # Buy PUT
            strike = round(close / STRIKE_INTERVAL) * STRIKE_INTERVAL
            inst = get_option_instrument(strike, 'PUT')
            if inst:
                response = place_order(inst['token'], "BUY", LOT_SIZE, inst['symbol'])
                if response['status']:
                    entry_price = get_ltp(inst['token'])
                    position = {
                        'type': 'PUT',
                        'strike': strike,
                        'instrument_token': inst['token'],
                        'symbol': inst['symbol'],
                        'entry_price': entry_price,
                        'entry_time': time
                    }
                    put_traded_today = True
                    logging.info(f"Bought PUT {strike} at {entry_price}")
    else:
        # Manage position
        current_price = get_ltp(position['instrument_token'])
        profit = current_price - position['entry_price']  # Profit when price increases (since buying)
        if profit >= TARGET:
            # Target hit, sell back
            place_order(position['instrument_token'], "SELL", LOT_SIZE, position['symbol'])
            logging.info(f"Target hit for {position['type']} position (Strike: {position['strike']}), profit {profit}")
            position = None
        elif profit <= -STOP_LOSS:
            # Stop loss, sell back
            place_order(position['instrument_token'], "SELL", LOT_SIZE, position['symbol'])
            logging.info(f"Stop loss hit for {position['type']} position (Strike: {position['strike']}), loss {profit}")
            position = None
        elif time.time() >= EXIT_TIME:
            # Exit
            place_order(position['instrument_token'], "SELL", LOT_SIZE, position['symbol'])
            logging.info(f"Exit at {time} for {position['type']} position (Strike: {position['strike']}), profit {profit}")
            position = None

def on_open(ws):
    global start_time, last_candle_time, first_candle_high, first_candle_low
    logging.info("WebSocket opened")
    today = datetime.date.today()
    market_open_dt = datetime.datetime.combine(today, market_open)
    current_dt = datetime.datetime.now()
    if current_dt >= market_open_dt + first_candle_duration:
        # Started after 9:45, fetch historical first candle
        historical = get_historical_candle(BANKNIFTY_TOKEN, market_open_dt, market_open_dt + first_candle_duration)
        if historical:
            first_candle_high = historical['high']
            first_candle_low = historical['low']
            logging.info(f"First candle high: {first_candle_high}, low: {first_candle_low} (from historical data)")
            start_time = current_dt
            last_candle_time = current_dt
        else:
            logging.error("Failed to fetch historical first candle. Cannot proceed.")
            return
    else:
        # Started before or during first 30 min, set start_time to market open
        start_time = market_open_dt
        last_candle_time = start_time
    # Subscribe to BankNifty
    subscribe_data = {
        "action": 1,  # Subscribe
        "key": [f"NSE|{BANKNIFTY_TOKEN}"]  # BankNifty
    }
    ws.send(json.dumps(subscribe_data))

def on_error(ws, error):
    logging.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.info("WebSocket closed")

def start_websocket():
    global web_socket
    web_socket = SmartWebSocketV2(
        auth_token=smart_api.access_token,
        api_key=API_KEY,
        client_code=CLIENT_ID,
        feed_token=smart_api.feed_token
    )
    web_socket.on_open = on_open
    web_socket.on_message = on_message
    web_socket.on_error = on_error
    web_socket.on_close = on_close
    web_socket.connect()

def main():
    if not authenticate():
        return
    start_websocket()
    # Keep running
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()