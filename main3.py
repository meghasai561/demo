import datetime
import time
import json
import logging
import pyotp
import requests
from datetime import timedelta
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import zoneinfo

# ─────────────────────────────────────────────
# IST Logging Setup
# ─────────────────────────────────────────────

class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = datetime.datetime.fromtimestamp(
            record.created, tz=zoneinfo.ZoneInfo("Asia/Kolkata")
        )
        return ct.strftime(datefmt or self.default_time_format)

logging.basicConfig(
    filename="trading_log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
for handler in logging.getLogger().handlers:
    handler.setFormatter(
        ISTFormatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    )

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

API_KEY     = "UqpiUvRZ"
CLIENT_ID   = "S387905"
PASSWORD    = "5612"
TOTP_SECRET = "PTHQZWA2P75ES2ENO3UILLSAJY"   # From your AngelOne QR code
IS_PAPER    = True               # Set False for live trading

# Trading Parameters
ENTRY_THRESHOLD = 35
TARGET          = 10
STOP_LOSS       = 35
LOT_SIZE        = 30
STRIKE_INTERVAL = 100
NO_ENTRY_AFTER  = datetime.time(15, 5)
EXIT_TIME       = datetime.time(15, 15)
MARKET_OPEN     = datetime.time(9, 15)
MARKET_CLOSE    = datetime.time(15, 30)

BANKNIFTY_INDEX_TOKEN = "99926009"   # BankNifty index token on NSE_CM
EXCHANGE_TYPE_NSE_CM  = 1            # NSE Cash/Index
EXCHANGE_TYPE_NSE_FO  = 2            # NSE F&O (for options orders)

# Subscription modes
MODE_LTP        = 1
MODE_QUOTE      = 2
MODE_SNAP_QUOTE = 3   # Full OHLC + depth — use this for candle building

# ─────────────────────────────────────────────
# Global State
# ─────────────────────────────────────────────

smart_api          = None
sws                = None          # Single SmartWebSocketV2 instance

# Session tokens — populated by create_session()
jwt_token          = None
refresh_token      = None
feed_token         = None

# Strategy state
first_candle_high  = None
first_candle_low   = None
position           = None
candle_data        = []
current_expiry     = None
call_traded_today  = False
put_traded_today   = False
instruments        = []
start_time         = None
last_candle_time   = None

first_candle_duration      = timedelta(minutes=30)
subsequent_candle_duration = timedelta(minutes=3)

# ─────────────────────────────────────────────
# Step 1 — Create Session (JWT + Feed Token)
# ─────────────────────────────────────────────

def create_session():
    """
    Authenticates with AngelOne SmartAPI and populates:
        jwt_token, refresh_token, feed_token, smart_api
    Returns True on success, False on failure.
    """
    global smart_api, jwt_token, refresh_token, feed_token

    smart_api = SmartConnect(api_key=API_KEY)

    # Generate one-time TOTP pin
    totp     = pyotp.TOTP(TOTP_SECRET)
    totp_pin = totp.now()
    logging.info("Generated TOTP pin")

    # Generate session — returns a dict with status + data
    session = smart_api.generateSession(CLIENT_ID, PASSWORD, totp_pin)

    if not session or not session.get("status"):
        logging.error(f"Session creation failed: {session.get('message', 'Unknown error')}")
        return False

    session_data  = session["data"]

    # ── Extract JWT token ──────────────────────────────────────
    jwt_token     = session_data["jwtToken"]      # Bearer token for REST calls
    refresh_token = session_data["refreshToken"]  # Use to renew without re-login

    # ── Set JWT on the SmartConnect instance ──────────────────
    smart_api.setAccessToken(jwt_token)

    # ── Extract Feed Token (required for WebSocket) ───────────
    feed_token    = smart_api.getfeedToken()

    logging.info("Session created successfully")
    logging.info(f"JWT Token    : {jwt_token[:20]}...  (truncated)")
    logging.info(f"Feed Token   : {feed_token}")
    logging.info(f"Refresh Token: {refresh_token[:20]}...  (truncated)")

    return True


# ─────────────────────────────────────────────
# Step 2 — Fetch Instruments + Current Expiry
# ─────────────────────────────────────────────

def fetch_instruments():
    """
    Downloads the NFO scrip master and finds the nearest BankNifty expiry.
    Populates: instruments, current_expiry
    """
    global instruments, current_expiry

    try:
        url      = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=15)

        if response.status_code != 200:
            logging.error(f"Failed to fetch instruments, HTTP {response.status_code}")
            return False

        all_instruments = response.json()
        instruments     = [i for i in all_instruments if i.get("exch_seg") == "NFO"]
        logging.info(f"Fetched {len(instruments)} NFO instruments")

        # Find nearest BankNifty expiry
        banknifty_opts = [i for i in instruments if i.get("name") == "BANKNIFTY"]
        expiries       = []

        for opt in banknifty_opts:
            try:
                exp_date = datetime.datetime.strptime(opt["expiry"], "%d%b%Y").date()
                expiries.append(exp_date)
            except (ValueError, KeyError):
                pass

        if expiries:
            current_expiry = min(expiries)
            logging.info(f"Nearest BankNifty expiry: {current_expiry}")
        else:
            logging.warning("No BankNifty expiry dates found")

        return True

    except Exception as e:
        logging.error(f"Error fetching instruments: {e}")
        return False


# ─────────────────────────────────────────────
# Step 3 — WebSocket Callbacks
# ─────────────────────────────────────────────

def on_open(wsapp):
    """Called when WebSocket connection is established."""
    global start_time, last_candle_time

    logging.info("WebSocket opened — subscribing to BankNifty ticks")

    ist            = zoneinfo.ZoneInfo("Asia/Kolkata")
    today          = datetime.date.today()
    market_open_dt = datetime.datetime.combine(today, MARKET_OPEN, tzinfo=ist)
    current_dt     = datetime.datetime.now(ist)

    if current_dt < market_open_dt + first_candle_duration:
        start_time = market_open_dt
    else:
        start_time = current_dt

    last_candle_time = start_time

    # ── Subscribe using the SDK method (NOT ws.send with raw JSON) ──
    correlation_id = "bnkfty01"   # Max 10 chars, unique per subscription group
    mode           = MODE_SNAP_QUOTE

    token_list = [
        {
            "exchangeType": EXCHANGE_TYPE_NSE_CM,  # Integer, not string "NSE"
            "tokens":       [BANKNIFTY_INDEX_TOKEN]
        }
    ]

    sws.subscribe(correlation_id, mode, token_list)
    logging.info(f"Subscribed: token={BANKNIFTY_INDEX_TOKEN}, mode={mode}")


def on_data(wsapp, message):

    print("-----------------------on data---------------------------")
    print(message)
    """
    Called for every incoming tick.
    `message` is already a parsed dict — no json.loads() needed.
    Typical keys: last_traded_price, open_price_of_the_day,
                  high_price_of_the_day, low_price_of_the_day,
                  exchange_timestamp, token
    """
    try:
        ltp = message.get("last_traded_price", 0) / 100  # SDK returns paise → convert to ₹
        logging.info(f"Tick | LTP: {ltp} | Raw: {message}")
        process_tick(message)
    except Exception as e:
        logging.error(f"Error in on_data: {e}")


def on_error(wsapp, error):
    logging.error(f"WebSocket error: {error}")


def on_close(wsapp, *args):
    logging.info(f"WebSocket closed: {args}")


# ─────────────────────────────────────────────
# Step 4 — Create & Start WebSocket
# ─────────────────────────────────────────────

def start_websocket():
    """
    Creates a single SmartWebSocketV2 instance using the tokens
    obtained from create_session(), wires up callbacks, and connects.
    """
    global sws

    if not jwt_token or not feed_token:
        logging.error("Cannot start WebSocket — session tokens missing. Call create_session() first.")
        return

    sws = SmartWebSocketV2(
        auth_token = jwt_token,    # jwtToken from session
        api_key    = API_KEY,
        client_code= CLIENT_ID,
        feed_token = feed_token    # from smart_api.getfeedToken()
    )

    # ── Wire callbacks ─────────────────────────────────────────
    sws.on_open  = on_open
    sws.on_data  = on_data    # ← MUST be on_data, NOT on_message
    sws.on_error = on_error
    sws.on_close = on_close

    logging.info("Connecting WebSocket...")
    sws.connect()


# ─────────────────────────────────────────────
# Strategy Logic (Stub — fill in your logic)
# ─────────────────────────────────────────────

def process_tick(tick: dict):
    """
    Called for every tick. Add your candle-building and
    entry/exit logic here.
    """
    ist        = zoneinfo.ZoneInfo("Asia/Kolkata")
    now        = datetime.datetime.now(ist)
    ltp        = tick.get("last_traded_price", 0) / 100

    # TODO: build 30-min first candle, then 3-min candles
    # TODO: check entry conditions vs first_candle_high / first_candle_low
    # TODO: manage open position (target / stop-loss / time-based exit)
    pass


# ─────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────

def main():
    # 1. Authenticate and get JWT + Feed Token
    if not create_session():
        logging.error("Aborting — session creation failed")
        return

    # 2. Load instrument master (expiry, option tokens)
    if not fetch_instruments():
        logging.warning("Instruments not loaded — continuing without expiry info")

    # 3. Start WebSocket (subscribes in on_open)
    start_websocket()

    # 4. Keep process alive; all work happens in on_data callbacks
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()