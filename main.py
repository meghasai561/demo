import datetime
import time
import threading
import logging
import requests
import pandas as pd
import pyotp
from dataclasses import dataclass
from typing import Optional
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# ===================================
# CONFIG
# ===================================

LIVE_MODE = False

API_KEY = "UqpiUvRZ"
CLIENT_ID = "S387905"
PASSWORD = "5612"
TOTP = "PTHQZWA2P75ES2ENO3UILLSAJY"

LOT_SIZE = 30
TARGET_POINTS = 30
SL_POINTS = 30
BREAKOUT_BUFFER = 25

ENTRY_CUTOFF = datetime.time(15, 10)
FORCE_EXIT = datetime.time(15, 20)

LOG_FILE = "banknifty_production.log"

# ===================================
# LOGGING
# ===================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

logger = logging.getLogger()

# ===================================
# DATA STRUCTURES
# ===================================

@dataclass
class ActiveTrade:
    symbol: str
    token: str
    entry_price: float
    qty: int
    sl_order_id: Optional[str] = None
    target_order_id: Optional[str] = None
    entry_order_id: Optional[str] = None
    active: bool = True

# ===================================
# MAIN ENGINE
# ===================================

class BankNiftyEngine:

    def __init__(self):

        self.api = SmartConnect(api_key=API_KEY)
        # use the configured TOTP secret
        totp = pyotp.TOTP(TOTP)
        TOTP_PIN = totp.now()
        session = self.api.generateSession(CLIENT_ID, PASSWORD, TOTP_PIN)
        refresh = session['data']['refreshToken']
        self.api.generateToken(refresh)

        logger.info("Login Successful")

        self.instrument_df = self.load_instruments()

        self.range_high = None
        self.range_low = None

        self.candles = []
        self.current_candle = None
        self.last_candle_time = None

        self.trade_ce_done = False
        self.trade_pe_done = False

        self.active_trade: Optional[ActiveTrade] = None

        self.banknifty_token = self.get_index_token()

        feedToken = self.api.getfeedToken()

        self.api.feed_token = feedToken
        print("Before Websocket Init")
        # Websocket requires individual tokens instead of the SmartConnect object
        self.ws = SmartWebSocketV2(
            self.api.access_token,
            API_KEY,
            self.api.userId,
            feed_token=self.api.feed_token
        )
        print("After Websocket Init")
        self.ws.on_data = self.on_tick
        self.ws.on_open = lambda ws: logger.info("Websocket Connected")
        self.ws.on_error = lambda ws, err: logger.error(err)

    # ===================================
    # INSTRUMENT LOADING
    # ===================================

    def load_instruments(self):
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        df = pd.DataFrame(requests.get(url).json())
        logger.info("Instrument master loaded")
        return df

    def get_index_token(self):
        row = self.instrument_df[
            (self.instrument_df['name'] == 'BANKNIFTY') &
            (self.instrument_df['instrumenttype'] == 'AMXIDX')
        ].iloc[0]
        return row['token']

    # ===================================
    # CANDLE ENGINE (3 MIN)
    # ===================================

    def on_tick(self, ws, message):

        ltp = float(message['last_traded_price']) / 100
        now = datetime.datetime.now()

        # 30 min range capture
        if now.time() <= datetime.time(9, 45):
            self.update_range(ltp)

        # 3 min candle builder
        self.build_3m_candle(ltp, now)

        # Force exit
        if now.time() >= FORCE_EXIT:
            self.force_exit()

    def build_3m_candle(self, ltp, now):

        minute = (now.minute // 3) * 3
        candle_time = now.replace(minute=minute, second=0, microsecond=0)

        if self.current_candle is None:
            self.current_candle = {
                "time": candle_time,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp
            }
            return

        if candle_time != self.current_candle["time"]:
            closed_candle = self.current_candle
            self.candles.append(closed_candle)

            logger.info(f"3m Candle Closed: {closed_candle}")

            self.evaluate_signal(closed_candle["close"])

            self.current_candle = {
                "time": candle_time,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp
            }
        else:
            self.current_candle["high"] = max(self.current_candle["high"], ltp)
            self.current_candle["low"] = min(self.current_candle["low"], ltp)
            self.current_candle["close"] = ltp

    # ===================================
    # RANGE LOGIC
    # ===================================

    def update_range(self, ltp):
        if self.range_high is None or ltp > self.range_high:
            self.range_high = ltp
        if self.range_low is None or ltp < self.range_low:
            self.range_low = ltp

    # ===================================
    # SIGNAL
    # ===================================

    def evaluate_signal(self, close_price):

        now = datetime.datetime.now().time()

        if now >= ENTRY_CUTOFF:
            return

        if self.range_high is None:
            return

        if close_price >= self.range_high + BREAKOUT_BUFFER and not self.trade_pe_done:
            self.enter_trade(close_price, "PE")
            self.trade_pe_done = True

        elif close_price <= self.range_low - BREAKOUT_BUFFER and not self.trade_ce_done:
            self.enter_trade(close_price, "CE")
            self.trade_ce_done = True

    # ===================================
    # ORDER MANAGEMENT
    # ===================================

    def enter_trade(self, ltp, option_type):

        strike = int(round(ltp / 100) * 100)
        expiry = self.get_expiry()

        row = self.instrument_df[
            (self.instrument_df['name'] == 'BANKNIFTY') &
            (self.instrument_df['strike'] == strike * 100) &
            (self.instrument_df['expiry'] == expiry) &
            (self.instrument_df['symbol'].str.endswith(option_type))
        ].iloc[0]

        symbol = row['symbol']
        token = row['token']

        order = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": "SELL",
            "exchange": "NFO",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": LOT_SIZE
        }

        entry_id = self.api.placeOrder(order)

        logger.info(f"Entry order placed {entry_id}")

        entry_price = self.get_order_price(entry_id)

        sl_id = self.place_sl(symbol, token, entry_price)
        tgt_id = self.place_target(symbol, token, entry_price)

        self.active_trade = ActiveTrade(
            symbol=symbol,
            token=token,
            entry_price=entry_price,
            qty=LOT_SIZE,
            entry_order_id=entry_id,
            sl_order_id=sl_id,
            target_order_id=tgt_id
        )

        threading.Thread(target=self.monitor_orders).start()

    def place_sl(self, symbol, token, entry):
        order = {
            "variety": "STOPLOSS",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": "BUY",
            "exchange": "NFO",
            "ordertype": "STOPLOSS_LIMIT",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": entry + SL_POINTS + 2,
            "triggerprice": entry + SL_POINTS,
            "quantity": LOT_SIZE
        }
        return self.api.placeOrder(order)

    def place_target(self, symbol, token, entry):
        order = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": "BUY",
            "exchange": "NFO",
            "ordertype": "LIMIT",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": entry - TARGET_POINTS,
            "quantity": LOT_SIZE
        }
        return self.api.placeOrder(order)

    # ===================================
    # OCO SIMULATION
    # ===================================

    def monitor_orders(self):

        while self.active_trade and self.active_trade.active:
            orders = self.api.orderBook()['data']

            sl_status = self.get_status(self.active_trade.sl_order_id, orders)
            tgt_status = self.get_status(self.active_trade.target_order_id, orders)

            if sl_status == "complete":
                logger.info("SL HIT")
                self.cancel_order(self.active_trade.target_order_id)
                self.active_trade.active = False

            elif tgt_status == "complete":
                logger.info("TARGET HIT")
                self.cancel_order(self.active_trade.sl_order_id)
                self.active_trade.active = False

            time.sleep(1)

    def get_status(self, order_id, orders):
        for o in orders:
            if o['orderid'] == order_id:
                return o['status']
        return None

    def cancel_order(self, order_id):
        try:
            self.api.cancelOrder(order_id)
            logger.info(f"Order Cancelled {order_id}")
        except:
            pass

    # ===================================
    # FORCE EXIT
    # ===================================

    def force_exit(self):
        if self.active_trade and self.active_trade.active:
            logger.info("Force Exit Triggered")
            self.api.placeOrder({
                "variety": "NORMAL",
                "tradingsymbol": self.active_trade.symbol,
                "symboltoken": self.active_trade.token,
                "transactiontype": "BUY",
                "exchange": "NFO",
                "ordertype": "MARKET",
                "producttype": "INTRADAY",
                "duration": "DAY",
                "quantity": LOT_SIZE
            })
            self.active_trade.active = False

    # ===================================
    # EXPIRY
    # ===================================

    def get_expiry(self):
        today = datetime.date.today()
        expiries = sorted(self.instrument_df[
            self.instrument_df['name'] == 'BANKNIFTY'
        ]['expiry'].unique())

        expiries = [datetime.datetime.strptime(e, "%d%b%Y").date() for e in expiries]

        if today in expiries:
            expiries.remove(today)

        for e in expiries:
            if e >= today:
                return e.strftime("%d%b%Y").upper()

    # ===================================
    # RUN
    # ===================================

    def run(self):
        self.ws.connect()


# ===================================
# START
# ===================================

if __name__ == "__main__":
    engine = BankNiftyEngine()
    engine.run()
