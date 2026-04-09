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


API_KEY = "UqpiUvRZ"
CLIENT_ID = "S387905"
PASSWORD = "5612"
TOTP_SECRET = "PTHQZWA2P75ES2ENO3UILLSAJY" 

BANKNIFTY_TOKEN = "26009"  # BankNifty index token
EXCHANGE = "NSE"
CLIENT_CODE = "S387905"

correlation_id = "abc123"
action = 1
mode = 1
sws = None
token_list = [
    {
        "exchangeType": 1,
        "tokens": ["26009"]
    }
]
smart_api = SmartConnect(api_key=API_KEY)
# Generate TOTP
totp = pyotp.TOTP(TOTP_SECRET)
TOTP_PIN = totp.now()
# Generate session
session = smart_api.generateSession(CLIENT_ID, PASSWORD, TOTP_PIN)
AUTH_TOKEN=session['data']['jwtToken']
smart_api.setAccessToken(session['data']['jwtToken'])
print("Session generated successfully")

feedToken = smart_api.getfeedToken()

smart_api.feed_token = feedToken

sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, smart_api.feed_token)

sws.connect()
class BankNiftyEngine:
    def __init__(self):
        
        sws.on_open = on_open
        print("BankNiftyEngine initialized")

    def on_open(wsapp):
            print("on open")
            some_error_condition = False
            if some_error_condition:
                error_message = "Simulated error"
                if hasattr(wsapp, 'on_error'):
                    wsapp.on_error("Custom Error Type", error_message)
            else:
                sws.subscribe(correlation_id, mode, token_list)
                # sws.unsubscribe(correlation_id, mode, token_list1)

if __name__ == "__main__":
    engine = BankNiftyEngine()
    engine.run()