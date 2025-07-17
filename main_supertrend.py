#!/usr/bin/env python
# coding: utf-8

# # STRATEGY ST & MA. Live & Paper Trading

# In[ ]:


from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
# import os.path  # To manage paths
# import sys  # To find out the script name (in argv[0])
import pandas as pd
# import matplotlib
from Trade_Live import Order, Trade
from Logger_Module import my_logger, LoggerThread
from Alice_Module import *
from pya3 import *
from Gen_Functions import *
# import talib
import pandas_ta as ta
import threading
import time
import os
import numpy as np
import googlesheet as gs


# In[ ]:


threads = []
def log(text, bot=False, fn="log", f_name="ST"):
    # fn = 'log'
    try:
        log_thread = LoggerThread(text=text, bot=bot, fn=fn, f_name=f_name)
        threads.append(log_thread)
        log_thread.start()
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)


# In[ ]:


log(f"############### New Log Initiated: {datetime.datetime.now().date()} ###############")
log("Modules imported successfully.")


# In[ ]:


def def_alert_candle(sma_signal, st_signal, high, low):
    fn = "def_alert_candle"
    global entry_value, buy_count, sell_count, buy_signal, sell_signal
    
    try:
        if sma_signal==1 and st_signal==1:
            if buy_count == 0:
                entry_value = round((high + low)/2, 2)
                buy_count +=1
                sell_count = 0 
                buy_signal = True
                sell_signal = False 
                log(f"Buy signal activated. Waiting for buy entry value: {entry_value}. H: {high} L: {low}", fn=fn, bot=True)
            else:
                buy_signal = True
                sell_signal = False
                buy_count += 1
                sell_count = 0
        elif sma_signal==-1 and st_signal==-1:    
            if sell_count == 0:
                entry_value = round((high + low)/2, 2)
                buy_count = 0
                sell_count += 1
                sell_signal = True
                buy_signal = False
                log(f"Sell signal activated. Waiting for sell entry value: {entry_value}. H: {high} L: {low}", fn=fn, bot=True)
            else:
                buy_signal = False
                sell_signal = True
                sell_count += 1
                buy_count = 0         
        else:
            reset() 
            buy_signal = False
            sell_signal = False
         
        log(f"low:{low}, high:{high}, buy signal: {buy_signal}, sell signal: {sell_signal}, ENTRY VALUE: {entry_value}.", fn=fn)
        # print(f"sell_count: {sell_count}, buy_count: {buy_count}" )
        
    except Exception as e:
        text = f"Error: {e}"
        log(text=text, fn=fn, bot=True)
            
def reset():
    global entry_value, buy_count, sell_count
    fn='reset'
    try:
        entry_value = 0
        buy_count = 0
        sell_count = 0
        log(f"Counting reset to zero.", fn=fn)
    except Exception as e:
        text = f"Error: {e}"
        log(text=text, fn=fn, bot=True)


# In[ ]:


# def def_alert_candle(buy, sell, high, low):
#     fn = "def_alert_candle"
#     global entry_value, buy_count, sell_count
    
#     try:
#         if buy is True:
#             if buy_count == 0:
#                 entry_value = round((high + low)/2, 2)
#                 buy_count +=1
#                 sell_count = 0
#                 log(f"Buy signal activated. Waiting for buy entry value: {entry_value}. H: {high} L: {low}", fn=fn, bot=True)
#             else:
#                 buy_count +=1
#         elif sell is True:
#             if sell_count == 0:
#                 entry_value = round((high + low)/2, 2)
#                 sell_count += 1
#                 buy_count = 0
#                 log(f"Sell signal activated. Waiting for sell entry value: {entry_value}. H: {high} L: {low}", fn=fn, bot=True)
#             else:
#                 sell_count +=1
#     except Exception as e:
#         text = f"Error: {e}"
#         log(text=text, fn=fn, bot=True)
            
# def reset(buy, sell):
#     global entry_value, buy_count, sell_count
#     fn='reset'
#     try:
#         if buy is False and sell is False:
#             entry_value = 0
#             buy_count = 0
#             sell_count = 0
#             log(f"Counting reset to zero.", fn=fn)
#     except Exception as e:
#         text = f"Error: {e}"
#         log(text=text, fn=fn, bot=True)


# In[ ]:


# calculate last candle time
def last_candle_time(timeframe):
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hr = now.hour
    mins = now.minute
    if mins<timeframe:
        mins = 60 - timeframe
        hr -= 1
    else:
        mins -= timeframe
    return datetime.datetime(year, month, day, hr, mins, 0)
    # return datetime.datetime(year, month, day, 15, 15, 0) #- datetime.timedelta(hours=5, minutes=30)


# ### FUNC to create OHLC from feed

# In[ ]:


def createOHLC():
    fn = "createOHLC"
    log("## create OHLC func called ##", fn=fn)
    start = time.time()
    global INTERVAL, TIMEFRAME, SESSION_END_TIME, bn_data, INITIAL_STATUS, buy_signal, sell_signal, entry_value
    
    try:
        if datetime.datetime.now().time() <= SESSION_END_TIME:
            df = pd.read_csv("26009.csv") # hardcore for Bank Nifty only. CSV containing live feed data of BN.
            df.set_index("ft", inplace=True)
            df.index = pd.to_datetime(df.index, unit='s')
            df.index = df.index + pd.Timedelta(hours=5, minutes=30)
            df = df['ltp'].resample('5min').ohlc() 
            df = df.drop(df[df.open.isnull()].index)
            bn.data = pd.concat([bn_data, df], axis = 0)
            
            # log("26009.csv and downloaded data combined & resampled to 5 Min." , fn=fn)
            
            # log("calc_indicator called.", fn=fn)
            calc_indicator() # to calculate indicators value & affix to bn_data
            
            candle_index = last_candle_time(5)
                     
            # Assigning low, high, close, open_, sma & st
            low = bn.data['low'][candle_index]
            high = bn.data['high'][candle_index]
            close = bn.data['close'][candle_index]
            open_ = bn.data['open'][candle_index]
            sma_signal = bn.data['sma'][candle_index]
            st_signal = bn.data['SUPERTd_21_2.0'][candle_index]
            
            # assigning buy & sell signals
#             if sma_signal==1 and st_signal==1:
#                 buy_signal = True
#             elif sma_signal == -1 or st_signal==-1:    
#                 buy_signal = False

#             if sma_signal == -1 and st_signal==-1:
#                 sell_signal = True
#             elif sma_signal == 1 or st_signal==1:    
#                 sell_signal = False
            
            # log(f"{candle_index}: low:{low}, high:{high}, buy: {buy_signal}, sell: {sell_signal}, ENTRY VALUE: {entry_value}.")
            
            # to define entry value (half of high & low of alert candle)
            
            # def_alert_candle(buy_signal, sell_signal, high, low)  
            def_alert_candle(sma_signal=sma_signal, st_signal=st_signal, high=high, low=low)
            
            INTERVAL = calc_next_5min() # set time to start next
            
            threading.Timer(INTERVAL, createOHLC).start()
            # INTERVAL = TIMEFRAME - (time.time() - start) # set time to start next
            log(f"Next run after {INTERVAL} secs", fn=fn)
            # threading.Timer(INTERVAL, createOHLC).start()
        else:
            log('******** create OHLC function ended. ****', fn=fn)

    except Exception as e:
        text = f"Error: {e}"
        log(text=text, fn=fn, bot=True)


# #### Calculating indicators value: sma5, sma20 and supertrend(21,3)

# In[ ]:


#### Calculating indicators value: sma5, sma20 and supertrend(21,3)
def calc_indicator():
    fn="calc_indicator"
    
    try:
        bn.data['sma5'] = talib.SMA(bn.data['close'], timeperiod=5)
        bn.data['sma20'] = talib.SMA(bn.data['close'], timeperiod=20)
        bn.data['sma'] = np.where(bn.data['sma5'] > bn.data['sma20'], 1, -1)
        
        # log("sma 5 & 20 calculated.", fn=fn)
        st = ta.supertrend(bn.data['high'], bn.data['low'], bn.data['close'], length=21, multiplier=2, offset=None)
        # log("ST calculated.", fn=fn)
        bn.data = pd.concat([bn.data, st], axis=1)
        log("sma and St indicators concatenated to main data bn.data.", fn=fn)
    except Exception as e:
        text = f"Error: {e}"
        log(text=text, fn=fn, bot=True)

    
# Function to take posn at ATM in PE & CE.
def entry_atm(bn_ltp, buy_sell):
    fn= 'entry_atm'
    log(f"{fn} Called.", fn=fn)
    global pe, ce

    try:
        ATM = strike_calc(ltp=bn_ltp, base=100, strike_difference=0)

        ce.instrument = ce.get_instrument_for_fno(symbol=SYMBOL, expiry_date=str(weekly_expiry_calculator()),
                                                          is_fut=False, strike=ATM-100, is_ce=True)
        pe.instrument = pe.get_instrument_for_fno(symbol=SYMBOL, expiry_date=str(weekly_expiry_calculator()),
                                                              is_fut=False, strike=ATM+100, is_ce=False)
        ce.assigned(QTY)
        pe.assigned(QTY)

        if buy_sell is Order.buy:
            ce.place_order(type_of_order=Order.buy, price=ce.ltp)
            pe.place_order(type_of_order=Order.sell, price=pe.ltp)
        elif buy_sell is Order.sell:
            ce.place_order(type_of_order=Order.sell, price=ce.ltp)
            pe.place_order(type_of_order=Order.buy, price=pe.ltp)
            
        subscribe()

        # ce.ltp =220
        # pe.ltp =200

        ltp_update()
        
    except Exception as e:
        text = f"Error: {e}"
        log(text=text, fn=fn, bot=True)

# Function to Exit posn in PE & CE.
def exit_positions():
    fn='exit_positions'
    global pe, ce
    
    try:
        text = "Exiting positions."
        log(text, fn=fn)
        # my_logger(data_to_log=text, fn=fn, bot=True)
        if ce.position is True:
            ce.place_order(type_of_order=Order.sqoff, price=ce.ltp)

        if pe.position is True:
            pe.place_order(type_of_order=Order.sqoff, price=pe.ltp)

        global trade_count
        trade_count += 1
        text = f"Trade no {trade_count} exited."
        log(text, fn=fn, bot=True)
        # my_logger(data_to_log=text, fn=fn, bot=True)

        report_send_pt()
    except Exception as e:
        text = f"Error: {e}"
        log(text=text, fn=fn, bot=True)


# ## Session id generation

# In[ ]:


# getting session id & alice object
alice = session_id_generate(1)
# if alice object remains None
if alice is None:
    alice = session_id_generate()
    logger.info(f"Session Id generated and alice var initialised: {alice}")


# ## Define Instruments

# In[ ]:


INDEX_SYMBOL = 'NIFTY BANK'
SYMBOL = 'BANKNIFTY' 


# In[ ]:


# INDEX_SYMBOL = 'NIFTY 50'
# SYMBOL = 'NIFTY'


# #### Set trading start time

# In[ ]:


# To keep track of trading time
WEBSOCKET_START_TIME = datetime.datetime.strptime("08:30:00", "%H:%M:%S").time()
SESSION_START_TIME= datetime.datetime.strptime("09:14:59", "%H:%M:%S").time()
SESSION_END_TIME= datetime.datetime.strptime("15:30:00", "%H:%M:%S").time()
TRADE_START_TIME = datetime.datetime.strptime("09:45:00", "%H:%M:%S").time() # 
LAST_ENTRY_TIME = datetime.datetime.strptime("15:10:00", "%H:%M:%S").time()
FIRST_CANDLE= datetime.datetime.strptime("09:20:00", "%H:%M:%S").time()
INITIAL_TIME= datetime.datetime.strptime("09:44:00", "%H:%M:%S").time()
SQUARED_OFF_TIME = datetime.datetime.strptime("15:25:00", "%H:%M:%S").time()
FIRST_TIME = datetime.datetime.strptime("09:30:00", "%H:%M:%S").time() # TO CHECK START TIME OF NEW DAY/ TO IGNORE THIS DAY
MONTHLY_EXPIRY = str(datetime.datetime(2023, 1, 25).date())
time_straddle = datetime.datetime.strptime("09:50:00", "%H:%M:%S").time() # time for straddle
DOWNLOAD_START_TIME= datetime.datetime.strptime("09:04:59", "%H:%M:%S").time()


# #### Required variables to set

# In[ ]:


SL = 0
TGT = 0
B_S = Order.sell
QTY = 50  # Trading qty
MODIFY_ON = 75
LTP_TGT_DIFF = 10
d = LTP_TGT_DIFF
BT = False
PAPER_TRADING = True
INTERVAL = 0  # variable to be set in createOHLC func to define next execution of self (in seconds)
TIMEFRAME = 300 # in secs
INITIAL_STATUS = None
buy_signal = None
sell_signal = None
trade_count = 0
first_trail = False
buy_count = 0
sell_count = 0
entry_value = 0
hundred_points_count = 0
skip = False


# #### Establish Trade Class

# In[ ]:


BN_INST = alice.get_instrument_by_symbol(exchange='INDICES', symbol=INDEX_SYMBOL)
bn = Trade(alice=alice, paper_trade=True)
bn.instrument = BN_INST
bn.feed = True # to storing feed data to csv
bn.assigned(QTY)
log("bn: Trade class established.")


# In[ ]:


while datetime.datetime.now().time() <= WEBSOCKET_START_TIME:
    sleep(30)

log("WEBSOCKET_START_TIME(08:30) crossed.", bot=True)

# code for connect websocket
alice_websocket()


# In[ ]:


##### Downloading data of last four days
##### To find indicators value

from_date = datetime.datetime.now().replace(hour=9, minute=14, second=0) - datetime.timedelta(days=4)  #four days back
to_date = datetime.datetime.now().replace(hour=15, minute=30, second=0) - datetime.timedelta(days=1)  # yesterday
log(f"Downloading dates:- from_date: {from_date} to_date: {to_date}.")

# waiting upto 0905 hrs
while datetime.datetime.now().time() <= DOWNLOAD_START_TIME:
    sleep(30)

bn_data = bn.historical_data(no_of_days=None, interval="1", indices=True,
                            from_datetime=from_date, to_datetime=to_date)
bn_data.set_index("datetime", inplace=True)
bn_data.index = pd.to_datetime(bn_data.index, format='%Y-%m-%d %H:%M:%S') # setting dates as Index
log("Historical data downloaded.")

#### Resample downloaded data to 5 min timeframe

bn_data = resample_feed(period='1min', data=bn_data) # resample to 1 min (to remove 59 sec)
remove_1530h(bn_data) # to remove 1530 min data
bn_data = resample_feed(period='5min', data=bn_data) # resample to 5 min timeframe
log("Downloaded data resampled to 5 min.")


# In[ ]:


while datetime.datetime.now().time() <= SESSION_START_TIME:
    sleep(1)

log(f"SESSION_STARTED: {SESSION_START_TIME}.", bot=True)

# subscribe for feeds (initially BN & Nifty)
subscribe()
    


# In[ ]:


ltp_update()


# In[ ]:


log("Initialising ce & pe")
ce = Trade(alice=alice, paper_trade=False)
ce.order_type = ProductType.Normal
pe = Trade(alice=alice, paper_trade=False)
pe.order_type = ProductType.Normal


# In[ ]:


# print(f"Current time in Sec: {datetime.datetime.now().second}")
# INTERVAL = TIMEFRAME - datetime.datetime.now().second
# sleep(INTERVAL)
while datetime.datetime.now().time() < FIRST_CANDLE:
    continue
log("Creating first OHLC.")
createOHLC()


# #### Strategy

# In[ ]:


### While loop
## STRATEGY
while True:
    # i+=1
    # print(i)
    fn = "Strategy"
    try:
        if datetime.datetime.now().time() <= INITIAL_TIME: # initialisation for initial signal
            if buy_signal:
                INITIAL_STATUS = Order.buy
            else:
                INITIAL_STATUS = Order.sell
            sleep(1)
            continue

        current_time = datetime.datetime.now().time() # variable for current time
        # print(current_time)

        # When Position is False
        if bn.position is False and current_time >= TRADE_START_TIME and current_time <= LAST_ENTRY_TIME:

            if INITIAL_STATUS is Order.buy:
                if sell_signal and entry_value < bn.ltp:
                    text = f'Initial Sell Signal Generated. Entry Value: {entry_value} BN Ltp: {bn.ltp}'
                    # logger.info(text)
                    log(text=text, fn=fn, bot=True)
                    bn.place_order(Order.sell, bn.ltp)
                    entry_atm(bn_ltp=bn.ltp, buy_sell=Order.sell)
                    INITIAL_STATUS = None
            elif INITIAL_STATUS is Order.sell:
                if buy_signal and entry_value > bn.ltp:
                    text = f'Initial Buy Signal Generated. Entry Value: {entry_value} BN Ltp: {bn.ltp}'
                    # logger.info(text)
                    log(text=text, fn=fn, bot=True)
                    bn.place_order(Order.buy, bn.ltp)
                    entry_atm(bn_ltp=bn.ltp, buy_sell=Order.buy)
                    INITIAL_STATUS = None
            elif INITIAL_STATUS is None:
                if buy_signal and entry_value > bn.ltp:
                    text = f'Buy Signal Generated. Entry Value: {entry_value} BN Ltp: {bn.ltp}'
                    # logger.info(text)
                    log(text=text, fn=fn, bot=True)
                    bn.place_order(Order.buy, bn.ltp)
                    entry_atm(bn_ltp=bn.ltp, buy_sell=Order.buy)
                if sell_signal and entry_value < bn.ltp:
                    text = f'Sell Signal Generated. Entry Value: {entry_value} BN Ltp: {bn.ltp}'
                    # logger.info(text)
                    log(text=text, fn=fn, bot=True)
                    bn.place_order(Order.sell, bn.ltp)
                    entry_atm(bn_ltp=bn.ltp, buy_sell=Order.sell)
     
        # When Position is True
        if bn.position is True:
            if bn.trade_type is Order.buy: 
                if buy_signal is False:
                    text = 'Exit Signal Generated.'
                    # logger.info(text)
                    log(text=text, fn=fn, bot=True)
                    bn.place_order(Order.sqoff, bn.ltp)
                    bn.square_off_open_orders() #sqoff all open orders
                    exit_positions()
                    
                    first_trail = False  
                    # reset(buy_signal, sell_signal) # buy_count = 0 & entry_value = 0
                elif bn.ltp - bn.entry_price > 100 and first_trail is False:
                    log(f"Mkt moved 100 pts after entry. Exiting PE", bot=True)
                    hundred_points_count += 1
                    # Exiting PE Sell position
                    pe.place_order(Order.sqoff, pe.ltp)
                    # Place SL of CE Buy position to entry price
                    # ce.sl = ce.entry_price
                    # ce.place_order(Order.sl, ce.ltp)
                    first_trail = True
            elif  bn.trade_type is Order.sell:
                if sell_signal is False:
                    text = 'Exit Signal Generated.'
                    # logger.info(text)
                    log(text=text, fn=fn, bot=True)
                    bn.place_order(Order.sqoff, bn.ltp)
                    bn.square_off_open_orders() #sqoff all open orders
                    exit_positions()
                    
                    first_trail = False    
                    # reset(buy_signal, sell_signal) # sell_count = 0 & entry_value = 0
                elif bn.entry_price - bn.ltp > 100 and first_trail is False:
                    log(f"Mkt moved 100 pts after entry. Exiting CE", bot=True)
                    hundred_points_count += 1
                    # Exiting CE Sell position
                    ce.place_order(Order.sqoff, ce.ltp)
                    # Place SL of PE Buy position to entry price
                    # pe.sl = pe.entry_price
                    # pe.place_order(Order.sl, pe.ltp)
                    first_trail = True

        # to SqOff
        if current_time >= SQUARED_OFF_TIME:
            if bn.position is True:   # To SqOff
                if current_time >= SQUARED_OFF_TIME:
                    text = f"SQ OFF time triggered: {SQUARED_OFF_TIME}. Breaking while loop"
                    # logger.info(text)
                    log(text=text, fn=fn, bot=True)
                    bn.place_order(Order.sqoff, bn.ltp)
                    exit_positions()
                    break
            else:
                log("SqOff time reached Without Position. Breaking while loop", fn=fn)
                entry_value = 0
                break
                
        # Sending report on every half an hour
        if (datetime.datetime.now().minute == 0 or datetime.datetime.now().minute == 30) and \
                datetime.datetime.now().second == 0:
            report_send()
            update_positions()
            # sleep(1)
        
    except Exception as e:
        text = f"Error: {e}"
        log(text=text, fn=fn, bot=True)


# In[ ]:


file_name = "SuperTrend.csv"
update_df(file_name)


# In[ ]:


# Waiting for session ending
while datetime.datetime.now().time() <= SESSION_END_TIME:
    sleep(60)

# Stopping websocket
try:
    alice.stop_websocket()
except Exception as e:
        text = f"Error: {e}"
        log(text=text, bot=True)

# sending docs to my Bot
bn.data.to_csv("ltp_data_st_ma.csv")
docs_to_send = ["ltp_data_st_ma.csv", "SuperTrend.csv", "data.txt", ]
docs_to_delete = ["data.txt", "26009.csv", "ltp_data_st_ma.csv"]
bot_token = '5398501864:AAFEn7ljDrKOVkXzhWX4P_khX9Xk-E8FicE'
url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
bot_chat_id = ['5162043562']

log("Docs sending to bot.")   
try:
    for item in docs_to_send:
        document = open(item, "rb")
        response = requests.post(url, data={'chat_id': bot_chat_id}, files={'document': document})
        log(response.json())
        log(f"{item} sent to Bot.")
except Exception as e:
    text = f"Error: {e}"
    log(text=text, bot=True)


sleep(300)

log("Deleting req files before closing.")

try:
    for item in docs_to_delete:
        os.remove(item)
        log(f"{item}: deleted")
except Exception as e:
    text = f"Error: {e}"
    log(text=text, bot=True)


# In[ ]:


# Update sheet of My account
try:
    update_trade_journal() 
    update_balance()

    # sleep(60)
    # To update trades log
    df = pd.read_csv("trade_journal.csv")
    print(df)
    sheet = gs.Spread("MYTradeAB", "Trades")
    sheet.update_df(df)

    # To update balance log
    df1 = pd.read_csv("balance.csv")
    print(df1)
    sheet1 = gs.Spread("MYTradeAB", "Balance") 
    # x = sheet1.worksheet_len()
    sheet1.update_df(df1)

    log("Balance and trade journal updated.")
except Exception as e:
    text = f"Error: {e}"
    log(text)


# In[ ]:


for t in threads:
    t.join()
sleep(300)
log("Script exiting.")

