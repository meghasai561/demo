import datetime
import time
import json
import pandas as pd
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Configuration - Replace with your actual credentials
API_KEY = "your_api_key"
CLIENT_ID = "your_client_id"
PASSWORD = "your_password"
TOTP_PIN = "your_totp_pin"  # Or generate dynamically
IS_PAPER = True  # Set to False for live trading

# Tokens
BANKNIFTY_TOKEN = "26009"  # BankNifty index token
EXCHANGE = "NSE"

# Trading Parameters
ENTRY_THRESHOLD = 35  # Points above/below 30-min high/low to enter
TARGET = 10  # Profit target in points
STOP_LOSS = 13  # Stop loss in points
LOT_SIZE = 25  # BankNifty options lot size
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
current_candle_start = None
candle_interval = 30  # Start with 30 min, then switch to 3 min
current_expiry = None
call_traded_today = False  # Track if CALL leg traded today
put_traded_today = False  # Track if PUT leg traded today
market_open = datetime.time(9, 15)
market_close = datetime.time(15, 30)

def authenticate():
    global smart_api, current_expiry
    smart_api = SmartConnect(api_key=API_KEY, isDemo=IS_PAPER)
    # Generate session
    data = smart_api.generateSession(CLIENT_ID, PASSWORD, TOTP_PIN)
    if data['status']:
        logging.info("Authentication successful")
        # Get current expiry for BankNifty
        instruments = smart_api.getInstruments('NFO')
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
        return True
    else:
        logging.error("Authentication failed")
        return False

def get_option_instrument(strike, option_type):
    global current_expiry
    if not current_expiry:
        return None
    expiry_str = current_expiry.strftime('%d%b%Y').upper()
    symbol = f"BANKNIFTY{expiry_str}{strike}{option_type[0]}E"  # PE or CE
    instruments = smart_api.getInstruments('NFO')
    for inst in instruments:
        if inst['symbol'] == symbol:
            return {'token': inst['token'], 'symbol': symbol}
    return None

def place_order(instrument_token, transaction_type, quantity, symbol):
    orderparams = {
        "variety": "NORMAL",
        "tradingsymbol": symbol,
        "symboltoken": instrument_token,
        "transactiontype": transaction_type,  # "SELL"
        "exchange": "NFO",
        "ordertype": "MARKET",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "quantity": quantity
    }
    response = smart_api.placeOrder(orderparams)
    return response

def get_ltp(instrument_token):
    ltp_data = smart_api.ltpData("NFO", instrument_token, "symbol")
    return ltp_data['data']['ltp']

def on_message(ws, message):
    global first_candle_high, first_candle_low, position, candle_data, current_candle_start, candle_interval
    data = json.loads(message)
    if 'data' in data:
        for tick in data['data']:
            timestamp = datetime.datetime.fromtimestamp(tick['timestamp'])
            ltp = tick['ltp']
            # For simplicity, use ltp as high/low/close for tick
            high = ltp
            low = ltp
            close = ltp

            # Aggregate into candles
            if current_candle_start is None:
                current_candle_start = timestamp.replace(minute=(timestamp.minute // candle_interval) * candle_interval, second=0, microsecond=0)
            if timestamp - current_candle_start >= datetime.timedelta(minutes=candle_interval):
                # Process previous candle
                if candle_data:
                    candle_high = max([d['high'] for d in candle_data])
                    candle_low = min([d['low'] for d in candle_data])
                    candle_close = candle_data[-1]['close']
                    process_candle(candle_high, candle_low, candle_close, current_candle_start)
                candle_data = []
                current_candle_start = timestamp.replace(minute=(timestamp.minute // candle_interval) * candle_interval, second=0, microsecond=0)
                if candle_interval == 30:
                    candle_interval = 3  # Switch to 3 min after first candle

            candle_data.append({'high': high, 'low': low, 'close': close, 'time': timestamp})

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
            logging.info(f"Target hit, profit {profit}")
            position = None
        elif profit <= -STOP_LOSS:
            # Stop loss, sell back
            place_order(position['instrument_token'], "SELL", LOT_SIZE, position['symbol'])
            logging.info(f"Stop loss hit, loss {profit}")
            position = None
        elif time.time() >= EXIT_TIME:
            # Exit
            place_order(position['instrument_token'], "SELL", LOT_SIZE, position['symbol'])
            logging.info(f"Exit at {time}, profit {profit}")
            position = None

def on_open(ws):
    logging.info("WebSocket opened")
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