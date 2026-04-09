"""
BankNifty Options Selling Strategy — AngelOne SmartAPI
======================================================
Strategy:
  - 30-min opening candle HIGH and LOW (9:15 to 9:45)
  - Watch 3-min candles after that
  - Close 35+ pts ABOVE 30min-high → SELL ATM PUT  (1 lot)
  - Close 35+ pts BELOW 30min-low  → SELL ATM CALL (1 lot)
  - Target: 30 pts | Stop Loss: 40 pts on option premium
  - Time exit at 3:15 PM | No new entries after 3:05 PM
  - One trade per leg per day

IS_PAPER = True  → Paper (simulated)
IS_PAPER = False → Live  (real orders)
command to run - nohup python main4.py &
"""

import datetime
import time
import logging
import pyotp
import requests
from dataclasses import dataclass
from typing import Optional
from datetime import timedelta
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import zoneinfo

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = datetime.datetime.fromtimestamp(record.created, tz=IST)
        return ct.strftime(datefmt or self.default_time_format)

_fmt = ISTFormatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
_file_h    = logging.FileHandler("trading_log.txt")
_console_h = logging.StreamHandler()
for _h in (_file_h, _console_h):
    _h.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_file_h, _console_h])
log = logging.getLogger("BNStrategy")

# ═══════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════

API_KEY     = "UqpiUvRZ"
CLIENT_ID   = "S387905"
PASSWORD    = "5612"
TOTP_SECRET = "PTHQZWA2P75ES2ENO3UILLSAJY"

# ── Toggle paper vs live ──────────────────────────────────
IS_PAPER = True   # True = Paper trading | False = Live trading
# ─────────────────────────────────────────────────────────

ENTRY_THRESHOLD = 35
TARGET          = 30
STOP_LOSS       = 40
LOT_SIZE        = 30
STRIKE_INTERVAL = 100

MARKET_OPEN      = datetime.time(9,  15)
FIRST_CANDLE_END = datetime.time(9,  45)
NO_ENTRY_AFTER   = datetime.time(15,  5)
EXIT_TIME        = datetime.time(15, 15)
MARKET_CLOSE     = datetime.time(15, 30)

FIRST_CANDLE_MINS = 30
SUBSEQUENT_MINS   = 3

BANKNIFTY_INDEX_TOKEN = "99926009"
EXCHANGE_NSE_CM       = 1   # NSE Cash/Index (integer)
EXCHANGE_NSE_FO       = 2   # NSE F&O options (integer)
MODE_LTP              = 1
MODE_SNAP_QUOTE       = 3

# ═══════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════

@dataclass
class Candle:
    open:  float
    high:  float
    low:   float
    close: float
    start: datetime.datetime
    end:   datetime.datetime

    def __str__(self):
        return (f"Candle [{self.start.strftime('%H:%M')}–{self.end.strftime('%H:%M')}]"
                f" O={self.open:.1f} H={self.high:.1f} L={self.low:.1f} C={self.close:.1f}")


@dataclass
class Position:
    leg:         str
    symbol:      str
    token:       str
    strike:      float
    entry_price: float
    entry_time:  datetime.datetime
    quantity:    int   = LOT_SIZE
    exit_price:  float = 0.0
    exit_reason: str   = ""
    is_open:     bool  = True

    @property
    def pnl_points(self):
        return self.entry_price - self.exit_price

    @property
    def pnl_rupees(self):
        return self.pnl_points * self.quantity


# ═══════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════

smart_api     = None
sws           = None
jwt_token     = None
refresh_token = None
feed_token    = None
instruments   = []
current_expiry = None

# Candle state
first_candle_high  = None
first_candle_low   = None
first_candle_done  = False
current_candle_open  = None
current_candle_high  = None
current_candle_low   = None
current_candle_start = None

# Trade state
call_traded_today = False
put_traded_today  = False
open_position     = None

# P&L
daily_pnl    = 0.0
closed_trades = []


# ═══════════════════════════════════════════════════════════
# STEP 1 — SESSION
# ═══════════════════════════════════════════════════════════

def create_session() -> bool:
    global smart_api, jwt_token, refresh_token, feed_token

    smart_api = SmartConnect(api_key=API_KEY)
    totp_pin  = pyotp.TOTP(TOTP_SECRET).now()
    log.info("Generating session...")

    session = smart_api.generateSession(CLIENT_ID, PASSWORD, totp_pin)
    if not session or not session.get("status"):
        log.error(f"Session failed: {session.get('message', 'unknown')}")
        return False

    data          = session["data"]
    jwt_token     = data["jwtToken"]       # Bearer token for REST API calls
    refresh_token = data["refreshToken"]   # Use to renew session at midnight

    smart_api.setAccessToken(jwt_token)    # Required before any REST call

    feed_token = smart_api.getfeedToken()  # Separate token for WebSocket auth

    log.info(f"Session OK  | JWT     : {jwt_token[:20]}...")
    log.info(f"            | FeedTok : {feed_token}")
    return True


# ═══════════════════════════════════════════════════════════
# STEP 2 — INSTRUMENTS
# ═══════════════════════════════════════════════════════════

def fetch_instruments() -> bool:
    global instruments, current_expiry

    url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            log.error(f"HTTP {r.status_code}")
            return False

        all_inst    = r.json()
        instruments = [i for i in all_inst if i.get("exch_seg") == "NFO"]
        log.info(f"Loaded {len(instruments)} NFO instruments")

        bn_opts  = [i for i in instruments if i.get("name") == "BANKNIFTY"]
        expiries = []
        for opt in bn_opts:
            try:
                expiries.append(datetime.datetime.strptime(opt["expiry"], "%d%b%Y").date())
            except Exception:
                pass

        if expiries:
            current_expiry = min(expiries)
            log.info(f"Nearest expiry : {current_expiry}")
        else:
            log.warning("Could not determine expiry date")
        return True
    except Exception as e:
        log.error(f"fetch_instruments error: {e}")
        return False


# ═══════════════════════════════════════════════════════════
# INSTRUMENT UTILITIES
# ═══════════════════════════════════════════════════════════

def get_atm_strike(ltp: float) -> float:
    return round(ltp / STRIKE_INTERVAL) * STRIKE_INTERVAL


def find_option(strike: float, option_type: str):
    if current_expiry is None:
        log.error("Expiry not set")
        return None

    # AngelOne has used different formats across scrip master versions
    # Try all known formats for the strike field
    strike_variants = [
        str(int(strike)),           # "52000"      ← most common
        str(int(strike * 100)),     # "5200000"    ← older format
        str(float(strike)),         # "52000.0"    ← rare
        f"{int(strike)}.00",        # "52000.00"   ← rare
    ]

    # AngelOne uses "OPTIDX" for index options, but some versions use "CE"/"PE"
    instrument_types = ["OPTIDX", option_type, "OPTSTK"]

    for inst in instruments:
        try:
            if inst.get("name") != "BANKNIFTY":
                continue

            # Check expiry
            inst_expiry = datetime.datetime.strptime(
                inst["expiry"], "%d%b%Y").date()
            if inst_expiry != current_expiry:
                continue

            # Check option type — symbol always ends in "CE" or "PE"
            if not inst.get("symbol", "").endswith(option_type):
                continue

            # Check strike against all known formats
            inst_strike = inst.get("strike", "")
            if inst_strike not in strike_variants:
                continue

            log.info(f"Option found: {inst['symbol']} | token={inst['token']} "
                     f"| strike_field='{inst_strike}' | type={inst.get('instrumenttype')}")
            return inst

        except Exception:
            continue

    # Not found — log a sample to help debug
    sample = next((i for i in instruments
                   if i.get("name") == "BANKNIFTY"
                   and i.get("symbol", "").endswith(option_type)), None)
    log.error(
        f"Option NOT found: BANKNIFTY {int(strike)} {option_type} exp={current_expiry}"
    )
    if sample:
        log.error(
            f"Sample {option_type} in scrip master → "
            f"strike='{sample.get('strike')}' "
            f"instrumenttype='{sample.get('instrumenttype')}' "
            f"symbol='{sample.get('symbol')}' "
            f"expiry='{sample.get('expiry')}'"
        )
    return None

def get_option_ltp_rest(token: str, symbol: str = ""):
    try:
        data = smart_api.ltpData("NFO", symbol, token)
        if data and data.get("status"):
            return float(data["data"]["ltp"])
    except Exception as e:
        log.error(f"LTP REST error: {e}")
    return None


# ═══════════════════════════════════════════════════════════
# ORDER MANAGER
# ═══════════════════════════════════════════════════════════

def _build_order_params(symbol, token, qty, side):
    return {
        "variety":         "NORMAL",
        "tradingsymbol":   symbol,
        "symboltoken":     token,
        "transactiontype": side,
        "exchange":        "NFO",
        "ordertype":       "MARKET",
        "producttype":     "INTRADAY",
        "duration":        "DAY",
        "price":           "0",
        "squareoff":       "0",
        "stoploss":        "0",
        "quantity":        str(qty),
    }


def place_sell_order(symbol: str, token: str, qty: int):
    ltp = get_option_ltp_rest(token, symbol)
    if ltp is None:
        log.error("Cannot fetch LTP — SELL order aborted")
        return None

    if IS_PAPER:
        log.info(f"[PAPER] SELL {qty} × {symbol} @ ₹{ltp:.2f}")
        return ltp

    try:
        resp = smart_api.placeOrder(_build_order_params(symbol, token, qty, "SELL"))
        if resp and resp.get("status"):
            log.info(f"[LIVE] SELL placed | {symbol} | orderid={resp['data']['orderid']}")
            time.sleep(1)
            return get_option_ltp_rest(token, symbol) or ltp
        log.error(f"SELL rejected: {resp}")
        return None
    except Exception as e:
        log.error(f"place_sell_order: {e}")
        return None


def place_buy_order(symbol: str, token: str, qty: int):
    ltp = get_option_ltp_rest(token, symbol)
    if ltp is None:
        log.error("Cannot fetch LTP — BUY order aborted")
        return None

    if IS_PAPER:
        log.info(f"[PAPER] BUY (exit) {qty} × {symbol} @ ₹{ltp:.2f}")
        return ltp

    try:
        resp = smart_api.placeOrder(_build_order_params(symbol, token, qty, "BUY"))
        if resp and resp.get("status"):
            log.info(f"[LIVE] BUY placed | {symbol} | orderid={resp['data']['orderid']}")
            time.sleep(1)
            return get_option_ltp_rest(token, symbol) or ltp
        log.error(f"BUY rejected: {resp}")
        return None
    except Exception as e:
        log.error(f"place_buy_order: {e}")
        return None


# ═══════════════════════════════════════════════════════════
# STRATEGY ENGINE
# ═══════════════════════════════════════════════════════════

def enter_trade(leg: str, banknifty_ltp: float):
    global open_position, call_traded_today, put_traded_today

    option_type = "PE" if leg == "PUT" else "CE"
    strike      = get_atm_strike(banknifty_ltp)

    log.info(f"Entry signal: {leg} | BankNifty={banknifty_ltp:.1f} | Strike={int(strike)}")

    inst = find_option(strike, option_type)
    if inst is None:
        log.error(f"Instrument not found — {leg} skipped")
        return

    entry_price = place_sell_order(inst["symbol"], inst["token"], LOT_SIZE)
    if entry_price is None:
        log.error("Order failed — no position opened")
        return

    open_position = Position(
        leg         = leg,
        symbol      = inst["symbol"],
        token       = inst["token"],
        strike      = strike,
        entry_price = entry_price,
        entry_time  = datetime.datetime.now(IST),
    )

    if leg == "PUT":
        put_traded_today  = True
    else:
        call_traded_today = True

    log.info(
        f"POSITION OPEN | {leg} {inst['symbol']} | "
        f"Entry=₹{entry_price:.2f} | "
        f"Target ≤ ₹{entry_price - TARGET:.2f} | "
        f"SL     ≥ ₹{entry_price + STOP_LOSS:.2f}"
    )

    # Subscribe option token to WebSocket for live P&L monitoring
    if sws:
        try:
            sws.subscribe(
                "optpos01",
                MODE_LTP,
                [{"exchangeType": EXCHANGE_NSE_FO, "tokens": [inst["token"]]}]
            )
            log.info(f"Subscribed option token={inst['token']} to WebSocket")
        except Exception as e:
            log.error(f"Option WS subscribe failed: {e}")


def exit_trade(reason: str, exit_price: float = None):
    global open_position, daily_pnl

    if open_position is None or not open_position.is_open:
        return

    pos = open_position

    if exit_price is None:
        exit_price = place_buy_order(pos.symbol, pos.token, pos.quantity)
    if exit_price is None:
        log.error("Exit price unavailable — using entry price")
        exit_price = pos.entry_price

    pos.exit_price  = exit_price
    pos.exit_reason = reason
    pos.is_open     = False
    daily_pnl      += pos.pnl_rupees
    closed_trades.append(pos)

    tag = "[PAPER]" if IS_PAPER else "[LIVE]"
    log.info(
        f"{tag} EXIT | {pos.leg} {pos.symbol} | "
        f"Entry=₹{pos.entry_price:.2f} → Exit=₹{exit_price:.2f} | "
        f"P&L = {pos.pnl_points:+.1f} pts  (₹{pos.pnl_rupees:+.1f}) | "
        f"Reason: {reason}"
    )
    log.info(f"Running daily P&L: ₹{daily_pnl:+.1f}")
    open_position = None


def check_exit_conditions(option_ltp: float):
    if open_position is None or not open_position.is_open:
        return

    pos   = open_position
    now_t = datetime.datetime.now(IST).time()

    # Time-based exit
    if now_t >= EXIT_TIME:
        log.info("3:15 PM — time exit")
        exit_trade("TIME_EXIT", option_ltp)
        return

    # Target hit: premium fell (we profit on short)
    if option_ltp <= pos.entry_price - TARGET:
        log.info(f"TARGET HIT | LTP={option_ltp:.2f} ≤ {pos.entry_price - TARGET:.2f}")
        exit_trade("TARGET", option_ltp)
        return

    # SL hit: premium rose (we lose on short)
    if option_ltp >= pos.entry_price + STOP_LOSS:
        log.info(f"STOP-LOSS HIT | LTP={option_ltp:.2f} ≥ {pos.entry_price + STOP_LOSS:.2f}")
        exit_trade("STOP_LOSS", option_ltp)
        return


def check_entry_on_candle_close(candle: Candle):
    if first_candle_high is None or first_candle_low is None:
        return

    if datetime.datetime.now(IST).time() >= NO_ENTRY_AFTER:
        return

    close = candle.close

    # Bullish breakout → sell PUT
    if not put_traded_today and close >= first_candle_high + ENTRY_THRESHOLD:
        log.info(
            f"PUT SIGNAL | close={close:.1f} ≥ "
            f"30min-high({first_candle_high:.1f}) + {ENTRY_THRESHOLD}"
        )
        enter_trade("PUT", close)

    # Bearish breakdown → sell CALL
    if not call_traded_today and close <= first_candle_low - ENTRY_THRESHOLD:
        log.info(
            f"CALL SIGNAL | close={close:.1f} ≤ "
            f"30min-low({first_candle_low:.1f}) - {ENTRY_THRESHOLD}"
        )
        enter_trade("CALL", close)


# ═══════════════════════════════════════════════════════════
# CANDLE BUILDER
# ═══════════════════════════════════════════════════════════

def get_candle_window(now: datetime.datetime):
    today        = now.date()
    mkt_open_dt  = datetime.datetime.combine(today, MARKET_OPEN,      tzinfo=IST)
    first_end_dt = datetime.datetime.combine(today, FIRST_CANDLE_END, tzinfo=IST)

    if now < first_end_dt:
        return mkt_open_dt, first_end_dt

    elapsed_secs = int((now - first_end_dt).total_seconds())
    candle_secs  = SUBSEQUENT_MINS * 60
    idx          = elapsed_secs // candle_secs
    c_start      = first_end_dt + timedelta(seconds=idx * candle_secs)
    c_end        = c_start + timedelta(seconds=candle_secs)
    return c_start, c_end


def process_index_tick(ltp: float, now: datetime.datetime):
    global first_candle_high, first_candle_low, first_candle_done
    global current_candle_open, current_candle_high, current_candle_low, current_candle_start

    candle_start, _ = get_candle_window(now)

    if current_candle_start is None or candle_start != current_candle_start:
        # Finalize previous candle
        if current_candle_start is not None:
            finished = Candle(
                open  = current_candle_open,
                high  = current_candle_high,
                low   = current_candle_low,
                close = ltp,
                start = current_candle_start,
                end   = candle_start,
            )
            on_candle_close(finished)

        # Start new candle
        current_candle_start = candle_start
        current_candle_open  = ltp
        current_candle_high  = ltp
        current_candle_low   = ltp
        log.info(f"New candle @ {candle_start.strftime('%H:%M')} | Open={ltp:.1f}")
    else:
        if ltp > current_candle_high:
            current_candle_high = ltp
        if ltp < current_candle_low:
            current_candle_low = ltp


def on_candle_close(candle: Candle):
    global first_candle_high, first_candle_low, first_candle_done

    log.info(f"CLOSED: {candle}")

    today        = candle.start.date()
    first_end_dt = datetime.datetime.combine(today, FIRST_CANDLE_END, tzinfo=IST)

    if not first_candle_done and candle.end <= first_end_dt + timedelta(seconds=5):
        # ── 30-min opening candle ─────────────────────────
        first_candle_high = candle.high
        first_candle_low  = candle.low
        first_candle_done = True
        log.info("=" * 55)
        log.info(f"30-MIN CANDLE | HIGH={first_candle_high:.1f}  LOW={first_candle_low:.1f}")
        log.info(f"PUT  entry if 3-min close ≥ {first_candle_high + ENTRY_THRESHOLD:.1f}")
        log.info(f"CALL entry if 3-min close ≤ {first_candle_low  - ENTRY_THRESHOLD:.1f}")
        log.info("=" * 55)
    else:
        # ── 3-min candle → check entries ──────────────────
        check_entry_on_candle_close(candle)


# ═══════════════════════════════════════════════════════════
# WEBSOCKET CALLBACKS
# ═══════════════════════════════════════════════════════════

def on_open(wsapp):
    log.info("WebSocket OPEN — subscribing to BankNifty index")
    sws.subscribe(
        "bnkfty01",
        MODE_SNAP_QUOTE,
        [{"exchangeType": EXCHANGE_NSE_CM, "tokens": [BANKNIFTY_INDEX_TOKEN]}]
    )


def on_data(wsapp, message):
    try:
        token = str(message.get("token", ""))
        ltp   = message.get("last_traded_price", 0) / 100   # paise → ₹
        now   = datetime.datetime.now(IST)

        if not (MARKET_OPEN <= now.time() <= MARKET_CLOSE):
            return

        if token == BANKNIFTY_INDEX_TOKEN:
            process_index_tick(ltp, now)
            # Safety: time-exit check on every index tick as well
            if open_position and open_position.is_open and now.time() >= EXIT_TIME:
                exit_trade("TIME_EXIT")

        elif open_position and token == open_position.token:
            log.debug(f"Option tick [{open_position.symbol}] ₹{ltp:.2f}")
            check_exit_conditions(ltp)

    except Exception as e:
        log.error(f"on_data error: {e}")


def on_error(wsapp, error):
    log.error(f"WebSocket error: {error}")


def on_close(wsapp, *args):
    log.warning(f"WebSocket closed: {args}")


# ═══════════════════════════════════════════════════════════
# WEBSOCKET STARTUP
# ═══════════════════════════════════════════════════════════

def start_websocket():
    global sws

    if not jwt_token or not feed_token:
        log.error("Tokens missing — call create_session() first")
        return

    sws = SmartWebSocketV2(
        auth_token  = jwt_token,
        api_key     = API_KEY,
        client_code = CLIENT_ID,
        feed_token  = feed_token,
    )
    sws.on_open  = on_open
    sws.on_data  = on_data      # Must be on_data (NOT on_message)
    sws.on_error = on_error
    sws.on_close = on_close

    log.info("Connecting WebSocket...")
    sws.connect()


# ═══════════════════════════════════════════════════════════
# END-OF-DAY REPORT
# ═══════════════════════════════════════════════════════════

def print_summary():
    mode = "PAPER" if IS_PAPER else "LIVE"
    log.info("=" * 60)
    log.info(f"  SUMMARY [{mode}]  {datetime.date.today()}")
    log.info(f"  30-min High : {first_candle_high}  |  Low : {first_candle_low}")
    log.info(f"  Trades      : {len(closed_trades)}")
    for t in closed_trades:
        log.info(
            f"  {t.leg:4s} | {t.symbol:30s} | "
            f"Entry=₹{t.entry_price:7.2f}  Exit=₹{t.exit_price:7.2f} | "
            f"P&L={t.pnl_points:+6.1f}pts (₹{t.pnl_rupees:+8.1f}) | {t.exit_reason}"
        )
    log.info(f"  Total P&L   : ₹{daily_pnl:+.1f}")
    log.info("=" * 60)


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    mode = "PAPER TRADING" if IS_PAPER else "LIVE TRADING"
    log.info(f"BankNifty Strategy | {mode}")
    log.info(f"Threshold={ENTRY_THRESHOLD}  Target={TARGET}  SL={STOP_LOSS}  Lot={LOT_SIZE}")

    if not create_session():
        log.error("Auth failed — exiting")
        return

    if not fetch_instruments():
        log.error("Instruments not loaded — cannot look up options, exiting")
        return

    start_websocket()

    try:
        while True:
            now = datetime.datetime.now(IST)
            if now.time() >= MARKET_CLOSE:
                if open_position and open_position.is_open:
                    log.warning("Market closed — force-closing position")
                    exit_trade("MARKET_CLOSE")
                print_summary()
                break
            time.sleep(5)

    except KeyboardInterrupt:
        log.info("Stopped by user")
        if open_position and open_position.is_open:
            exit_trade("USER_INTERRUPT")
        print_summary()


if __name__ == "__main__":
    main()
