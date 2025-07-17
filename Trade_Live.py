#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Live & Paper Trading Simulation & make a detailed report in a dataframe

from pya3 import *
import datetime
# from datetime import datetime, timedelta
# import math
import requests
from time import sleep
import calendar
import pandas as pd
from enum import Enum
from Logger_Module import *
import Alice_Module
import Gen_Functions
import os
import config
from My_Logger import setup_logger, LogLevel
logger = setup_logger(logger_name="Trade Live", log_level=LogLevel.INFO, log_to_console=config.print_logger)

# In[3]:


# Enums
class Order(Enum):
    buy = 'Buy'
    sell = 'Sell'
    sqoff = 'Sq Off'
    modify = 'Modify'
    sl = 'SL'
    tgt = 'TGT'
    cancel_sl = 'Cancel_SL'
    cancel_tgt = 'Cancel_TGT'
    # nrml = ProductType.Normal
    # mis = ProductType.Intraday


# In[4]:


class Trade:

    def __init__(self, alice, sl=0, tgt=0, paper_trade=False):

        self.alice = alice
        self.paper_trading = paper_trade
        self.data = None
        self.instrument = None
        self.exchange = None
        self.token = None
        self.symbol = None
        self.qty = None
        self.order_type = ProductType.Normal # Delivery // Product_Type.Normal
        self.ltp = None
        self.ltp_time = None
        self.ft = None
        self.feed = False  # for storing feed data to csv
        self.csv_file_existed = False
        self.trade_count = 0
        self.cash = None
        self.df = pd.DataFrame()
        self.status = ['order_not_initiated', 'order_initiated', 'open', 'trigger pending',
                       'complete', 'cancelled', 'rejected']  # For order status
        self.open_orders = ['open', 'trigger pending']
        self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
        self.order_details = {
            'entry': {
                'status': self.status[0],
                'price': None,
                'order_id': None,
                'tran_type': None,
                'dt': None
            },
            'exit': {  # used of sq off
                'status': self.status[0],
                'price': None,
                'order_id': None,
                'tran_type': None,
                'dt': None
            },
            'sl': {
                'status': self.status[0],
                'price': None,
                'order_id': None,
                'tran_type': None,
                'dt': None
            },
            'tgt': {
                'status': self.status[0],
                'price': None,
                'order_id': None,
                'tran_type': None,
                'dt': None
            }
        }

        # to evaluate sl & tgt
        self.sl_per = sl
        self.tgt_per = tgt
        # self.backtest = bt  # Variable to do only backtest not live trading

        # Variables to track position & its type
        self.trade_type = None  # B for 'Buy' & S for 'Sell'
        self.position = False  # True if trade is open else False

        # Variables for trading
        self.dt = None  # date with time
        self.tgt = None
        self.sl = None
        self.activate_tgt_trailing = False
        self.activate_sl_trailing = False
        self.pnl_trade = 0
        self.entry_price = None
        self.trade_dict = {'Inst': None, 'Entry_date': None, 'Buy_Sell1': None, 'Qty1': None, 'Price1': None,
                           'Tgt': None, 'SL': None,
                           'Exit_date': None, 'Buy_Sell2': None, 'Qty2': None, 'Price2': None, 'Pnl': 0, 'Fund': 0,
                           'Info': None}
        self.initialise_alice_variables()

    def initialise_alice_variables(self):
        Alice_Module.trade_class_list.append(self)

    def assigned(self, lots=1, qty_on_error=25):
        fn = 'assigned'
        try:
            self.exchange = self.instrument[0]
            self.token = self.instrument[1]
            self.symbol = self.instrument[3]
            if len(self.symbol) == 0:
                self.symbol = self.instrument[2]
            # t = datetime.datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
            text = f"{self.symbol} assigned."
            my_logger(data_to_log=text, fn=fn, bot=True)
            if self.feed:
                file_name = f"{self.token}.csv"
                result_file_exist = os.path.exists(file_name)
                if result_file_exist:
                    self.csv_file_existed = True
                    my_logger(data_to_log="csv existed", sym=self.symbol, fn=fn, bot=False)
            if self.instrument[5] == '':
                self.qty = qty_on_error
            else:
                self.qty = int(self.instrument[5] * lots)
        except Exception as e:
            t = datetime.datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
            text = f"{t}:Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)
            logger.exception(text)


    def order_place(self, transaction_type, order_type, product_type, price=0.0,
                    trigger_price=None, order_tag='order1', update_order_id_to=None):
        """function to place live order in AliceBlue & update the order id to respective order variable"""
        fn = "order_place"
        price = float(price)  # Making price as float value
        if trigger_price is not None:
            trigger_price = float(trigger_price)  # Making price as float value

        try:
            response = self.alice.place_order(transaction_type=transaction_type,
                                              instrument=self.instrument,
                                              quantity=self.qty,
                                              order_type=order_type,
                                              product_type=product_type,
                                              price=price,
                                              trigger_price=trigger_price,
                                              stop_loss=None,
                                              square_off=None,
                                              trailing_sl=None,
                                              is_amo=False,
                                              order_tag=order_tag)
            if response['stat'] == 'Ok':
                text = f"{update_order_id_to} order placed successfully at price: {price} | " \
                       f"trigger price: {trigger_price}. \n" \
                       f"Response: {response}"
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

                if update_order_id_to is not None:
                    self.update_order_id(update_order_id_to=update_order_id_to, order_id=response['NOrdNo'])
                else:
                    text = f"Update_order_id_to is None."
                    my_logger(data_to_log=text, fn=fn, bot=True)
            else:
                text = f"Check Status. Order response: {response}"
                my_logger(data_to_log=text, fn=fn, bot=True)
        except Exception as e:
            text = f"Error occurred during placing order. Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def update_order_id(self, update_order_id_to, order_id):
        fn = "update_order_id"
        try:
            self.order_details[update_order_id_to]["status"] = self.status[1]
            self.order_details[update_order_id_to]["order_id"] = order_id
        except Exception as e:
            # print(f"Error: {e}")
            my_logger(data_to_log=f"Error: {e}", fn=fn, bot=True)

    def order_cancel(self, order_id):
        fn = 'order_cancel'
        try:
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
            response = self.alice.cancel_order(order_id)  # Cancel an open order
            text = f"Response: {response}"
            my_logger(data_to_log=text, fn=fn, bot=True)
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)

    def order_modify(self, transaction_type, order_id, order_type, product_type, price=0.0,
                     trigger_price=None):
        fn = 'order_modify'
        try:
            price = float(price)
            if trigger_price is not None:
                trigger_price = float(trigger_price)

            response = self.alice.modify_order(transaction_type=transaction_type,
                                               instrument=self.instrument,
                                               order_id=order_id,
                                               quantity=self.qty,
                                               order_type=order_type,
                                               product_type=product_type,
                                               price=price,
                                               trigger_price=trigger_price)

            text = f"Modify: price: {price} | trigger price: {trigger_price}. Response: {response}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)

    # run order_check to check the status of orders & update order details
    def order_check(self, order_id=None):
        fn = "order_check"
        try:
            response = self.alice.get_order_history('')

            if type(response) is list:
                pending = []
                completed = []
                pending_order_id = []
                id_status = {}
                for i in range(len(response)):
                    data = {
                        "stat": response[i]['stat'],
                        "trigger_price": response[i]['Trgprc'],
                        "transaction_type": response[i]['Trantype'],  # 'S'/'B'
                        "trading_symbol": response[i]['Trsym'],
                        "remaining_quantity": response[i]['Unfilledsize'],
                        "rejection_reason": response[i]['RejReason'],
                        "quantity": response[i]['Qty'],
                        "product": response[i]['Pcode'],  # MIS/CC
                        "price": response[i]['Prc'],
                        "order_type": response[i]['Prctype'],  # mkt/limit
                        "order_tag": response[i]['remarks'],
                        "order_status": response[i]['Status'],
                        "order_entry_time": response[i]['iSinceBOE'],
                        "oms_order_id": response[i]['Nstordno'],
                        "nest_request_id": response[i]['RequestID'],
                        "lotsize": response[i]['multiplier'],
                        "login_id": response[i]['user'],
                        "leg_order_indicator": "",
                        "instrument_token": response[i]['token'],
                        "filled_quantity": response[i]['Fillshares'],
                        "exchange_time": response[i]['OrderedTime'],
                        "exchange_order_id": response[i]['ExchOrdID'],
                        "exchange": response[i]['Exchange'],
                        "disclosed_quantity": response[i]['Dscqty'],
                        "client_id": response[i]['accountId'],
                        "average_price": float(response[i]['Avgprc'])
                    }
                    id_status[response[i]['Nstordno']] = {"stat": response[i]['stat'],
                                                          "order_status": response[i]['Status'],
                                                          "trading_symbol": response[i]['Trsym'],
                                                          "average_price": float(response[i]['Avgprc']),
                                                          "trigger_price": response[i]['Trgprc'],
                                                          "product": response[i]['Pcode'],
                                                          "price": response[i]['Prc'],
                                                          "transaction_type": response[i]['Trantype']
                                                          }
                    if response[i]['Status'] == 'open' or response[i]['Status'] == 'trigger pending':
                        pending.append(data)
                        pending_order_id.append(data["oms_order_id"])

                    else:
                        completed.append(data)
                old_response = {
                    "stat": "Ok",
                    "message": "",
                    "data": {
                        "pending_orders": pending,
                        "completed_orders": completed,
                        "pending_oms_order_id": pending_order_id,
                        "id_status": id_status
                    }
                }
                #  def order_status(self): Update all the order id with their current status
                orders = old_response
                if orders['stat'] == 'Ok':
                    order_status = orders['data']['id_status']  # assign id_status to order_status

                    for items in self.order_details:
                        status = self.order_details[items]['status']
                        # if status is other than Not Initiated or 'complete'
                        if status != self.status[0] or status != self.status[4]:
                            # assigning order_id to variable from order detail
                            order_id = self.order_details[items]['order_id']

                            if order_id in order_status:  # updating status & avg price
                                self.order_details[items]['status'] = order_status[order_id]['order_status']
                                self.order_details[items]['price'] = order_status[order_id]['average_price']
                                self.order_details[items]['tran_type'] = order_status[order_id]['transaction_type']

                text = self.order_details
                # my_logger(data_to_log=text, fn=fn, bot=False)

                return old_response
            else:
                return response
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)

    def square_off_open_orders(self):  # pending to include square off of positions
        fn = 'square_off_open_orders'
        # Cancellation of all open orders including trigger pending orders
        try:
            response = self.order_check()
            order_ids = []
            if response['stat'] == 'Ok':
                for ids in response['data']['pending_oms_order_id']:
                    order_ids.append(ids)
                print(f"These Order Ids will be sent for cancellation: {order_ids}")
                for ids in order_ids:
                    self.order_cancel(order_id=ids)
            text = f"cancel order sent for: {order_ids}"
            my_logger(data_to_log=text, fn=fn, bot=True)
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)

    def square_off_open_positions(self):
        # Squared Off of all open positions
        # Change code to square off only mis positions
        fn = 'square_off_open_positions'
        try:
            net_position = self.alice.get_netwise_positions()
            if type(net_position) is list:
                pending_positions = [
                    {'exchange': data['Exchange'], 'token': int(data['Token']), 'net_qty': int(data['Netqty'])}
                    for data in net_position if data['Netqty'] != '0' and int(data['Token']) == self.token]
                # print(pending_positions)

                for items in pending_positions:

                    if self.quantity != abs(items['net_qty']):
                        print(f"Stg Qty: {self.quantity} || Pending Qty: {abs(items['net_qty'])}")
                        self.quantity = abs(items['net_qty'])
                        print(f" net qty: {items['net_qty']}")

                    if items['net_qty'] < 0:
                        print(f"{self.symbol}: buy order sent")
                        # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                        self.order_place(transaction_type=TransactionType.Buy, order_type=OrderType.Market, price=0.0,
                                         update_order_id_to=self.update_order_id_to[1])

                    else:
                        print(f"{self.symbol}: sell order sent")
                        # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                        self.order_place(transaction_type=TransactionType.Sell, order_type=OrderType.Market, price=0.0,
                                         update_order_id_to=self.update_order_id_to[1])
                text = f"Squared Off"
                my_logger(data_to_log=text, fn=fn, bot=True)
            else:
                text = net_position
                my_logger(data_to_log=text, fn=fn, bot=True)
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)

    def place_order(self, type_of_order, price=None, modify=None):
        fn = "place_order"
        current_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        try:
            self.trade_dict['Inst'] = self.symbol

            if type_of_order is Order.buy:  # used to take trade entry
                self.trade_dict['Entry_date'] = current_dt
                self.trade_dict['Buy_Sell1'] = type_of_order.value
                self.trade_dict['Qty1'] = self.qty
                self.trade_type = type_of_order
                if self.paper_trading is False:
                    # send a buy market order (self.update_order_id_to = ["entry", "exit", "sl", "tgt"])
                    self.order_place(transaction_type=TransactionType.Buy,
                                     order_type=OrderType.Market,
                                     product_type=self.order_type,
                                     price=0.0,
                                     trigger_price=None,
                                     update_order_id_to=self.update_order_id_to[0]  # entry
                                     )
                self.trade_count += 1

                self.entry_var(price)
                text = f"Buy order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.sell:  # used to take trade entry
                self.trade_dict['Entry_date'] = current_dt
                self.trade_dict['Buy_Sell1'] = type_of_order.value
                self.trade_dict['Qty1'] = self.qty * -1
                self.trade_type = type_of_order
                if self.paper_trading is False:
                    # send a sell market order
                    self.order_place(transaction_type=TransactionType.Sell,
                                     order_type=OrderType.Market,
                                     product_type=self.order_type,
                                     price=0.0,
                                     trigger_price=None,
                                     update_order_id_to=self.update_order_id_to[0]  # entry
                                     )
                self.trade_count += 1
                self.entry_var(price)
                text = f"Sell order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.sqoff:  # used to take trade exit
                self.trade_dict['Exit_date'] = current_dt

                if self.trade_type is Order.buy:  # if buy trade is in position
                    self.trade_dict['Buy_Sell2'] = Order.sell.value
                    self.trade_dict['Qty2'] = self.qty * -1
                    if self.paper_trading is False:
                        # send a sell order
                        self.order_place(transaction_type=TransactionType.Sell,
                                         order_type=OrderType.Market,
                                         product_type=self.order_type,
                                         price=0.0,
                                         trigger_price=None,
                                         update_order_id_to=self.update_order_id_to[1]  # exit
                                         )

                else:  # if sell trade is in position
                    self.trade_dict['Buy_Sell2'] = Order.buy.value
                    self.trade_dict['Qty2'] = self.qty
                    if self.paper_trading is False:
                        # send a buy order
                        self.order_place(transaction_type=TransactionType.Buy,
                                         order_type=OrderType.Market,
                                         product_type=self.order_type,
                                         price=0.0,
                                         trigger_price=None,
                                         update_order_id_to=self.update_order_id_to[1]  # exit
                                         )

                self.exit_var(type_of_order, price)
                text = f"SqOff order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.sl:  # used to take trade sl
                self.trade_dict['Exit_date'] = current_dt
                if self.trade_type is Order.buy:
                    self.trade_dict['Buy_Sell2'] = Order.sell.value
                    self.trade_dict['Qty2'] = self.qty * -1
                    if self.paper_trading is False:
                        # send a sl sell order
                        self.order_place(transaction_type=TransactionType.Sell,
                                         order_type=OrderType.StopLossLimit,
                                         product_type=self.order_type,
                                         price=self.sl - 5,
                                         trigger_price=self.sl,
                                         update_order_id_to=self.update_order_id_to[2]
                                         # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                                         )

                else:
                    self.trade_dict['Buy_Sell2'] = Order.buy.value
                    self.trade_dict['Qty2'] = self.qty
                    if self.paper_trading is False:
                        # send a sl buy order
                        self.order_place(transaction_type=TransactionType.Buy,
                                         order_type=OrderType.StopLossLimit,
                                         product_type=self.order_type,
                                         price=self.sl + 5,
                                         trigger_price=self.sl,
                                         update_order_id_to=self.update_order_id_to[2]
                                         # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                                         )
                self.exit_var(type_of_order, price)
                text = f"SL order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)
                
            elif type_of_order is Order.tgt:  # used to take trade tgt
                self.trade_dict['Exit_date'] = current_dt
                if self.trade_type is Order.buy:
                    self.trade_dict['Buy_Sell2'] = Order.sell.value
                    self.trade_dict['Qty2'] = self.qty * -1
                    if self.paper_trading is False:
                        # send a sell sl order
                        self.order_place(transaction_type=TransactionType.Sell,
                                         order_type=OrderType.StopLossLimit,
                                         product_type=self.order_type,
                                         price=self.tgt - 5,
                                         trigger_price=self.tgt,
                                         update_order_id_to=self.update_order_id_to[3]
                                         # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                                         )
                else:
                    self.trade_dict['Buy_Sell2'] = Order.buy.value
                    self.trade_dict['Qty2'] = self.qty
                    if self.paper_trading is False:
                        # send a buy market order
                        self.order_place(transaction_type=TransactionType.Buy,
                                         order_type=OrderType.StopLossLimit,
                                         product_type=self.order_type,
                                         price=self.tgt + 5,
                                         trigger_price=self.tgt,
                                         update_order_id_to=self.update_order_id_to[3]
                                         # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                                         )
                self.exit_var(type_of_order, price)
                text = f"Tgt order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)
                
            elif type_of_order is Order.modify:
                if modify == 'sl':
                    self.sl = price
                    if self.paper_trading is False:
                        # send a modify sl order
                        if self.trade_type is Order.sell:
                            self.order_modify(transaction_type=TransactionType.Buy,
                                              order_id=self.order_details['sl']['order_id'],
                                              order_type=OrderType.StopLossLimit,
                                              product_type=self.order_type,
                                              price=self.sl + 5,
                                              trigger_price=self.sl
                                              )
                        else:
                            self.order_modify(transaction_type=TransactionType.Sell,
                                              order_id=self.order_details['sl']['order_id'],
                                              order_type=OrderType.StopLossLimit,
                                              product_type=self.order_type,
                                              price=self.sl - 5,
                                              trigger_price=self.sl
                                              )

                elif modify == 'tgt':
                    self.tgt = price
                    if self.paper_trading is False:
                        # send a modify tgt order
                        if self.trade_type is Order.sell:
                            self.order_modify(transaction_type=TransactionType.Buy,
                                              order_id=self.order_details['tgt']['order_id'],
                                              order_type=OrderType.StopLossLimit,
                                              product_type=self.order_type,
                                              price=self.tgt + 5,
                                              trigger_price=self.tgt
                                              )
                        else:
                            self.order_modify(transaction_type=TransactionType.Sell,
                                              order_id=self.order_details['tgt']['order_id'],
                                              order_type=OrderType.StopLossLimit,
                                              product_type=self.order_type,
                                              price=self.tgt - 5,
                                              trigger_price=self.tgt
                                              )
                text = f"Modify order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.cancel_sl:
                if self.paper_trading is False and self.order_details['sl']['status'] in self.open_orders:
                    self.order_cancel(order_id=self.order_details['sl']['order_id'])
                    self.exit_var(type_of_order, price)
                    self.trade_log(f"{self.dt} cancel sl order sent")

            elif type_of_order is Order.cancel_tgt:
                if self.paper_trading is False and self.order_details['tgt']['status'] in self.open_orders:
                    self.order_cancel(order_id=self.order_details['tgt']['order_id'])
                    self.exit_var(type_of_order, price)
                    self.trade_log(f"{self.dt} cancel tgt order sent")
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def scrip_info(self):
        fn = 'scrip_info'
        # to get scrip info by providing proper scrip name
        try:
            info = self.alice.get_scrip_info(self.instrument)
            print(f"scrip info: {info}")
            high = info['High']
            low = info['Low']
            open = info['openPrice']
            prev_close = info['PrvClose']
            print(f"Open: {open}\n"
                  f"High: {high}\n"
                  f"Low : {low}\n"
                  f"Prev close: {prev_close}")
        except Exception as e:
            text = f"Error: {e}."
            my_logger(data_to_log=text, fn=fn, bot=True)

    def get_instrument(self, exchange, symbol):
        fn = 'get_instrument'
        try:
            instrument = self.alice.get_instrument_by_symbol(exchange, symbol)
            return instrument
        except Exception as e:
            text = f"Error: {e}."
            my_logger(data_to_log=text, fn=fn, bot=True)

    def get_instrument_for_fno(self, symbol, expiry_date, is_fut=False, strike=None, is_ce=False):
        fn = 'get_instrument_for_fno'
        try:
            instrument_fno = self.alice.get_instrument_for_fno(exch="NFO", symbol=symbol, expiry_date=expiry_date,
                                                               is_fut=is_fut, strike=strike, is_CE=is_ce)
            return instrument_fno
        except Exception as e:
            my_logger(data_to_log=f"Error: {e}.", fn=fn)

    def historical_data(self, no_of_days=None, interval="1", indices=False,
                        from_datetime=None, to_datetime=None):  # interval : ["1", "D"] // indices: True or False
        fn = "historical_data"
        """ from and to dates required """
        try:
            print(f"{self.symbol}: downloading historical data from {from_datetime} to {to_datetime}")
            response = self.alice.get_historical(self.instrument, from_datetime, to_datetime, interval, indices)
            return response
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)
            return None

    def pnl(self):
        """to update & calculate the pnl in df after closing of position"""
        fn = "pnl"
        try:
            if self.trade_dict['Buy_Sell1'] == 'Buy':
                pnl = round((self.trade_dict['Price2'] - self.trade_dict['Price1']) * self.trade_dict['Qty1'], 2)
                # self.trade_log(text=pnl, fn='pnl')
                return pnl
            else:
                pnl = round((self.trade_dict['Price1'] - self.trade_dict['Price2']) * self.trade_dict['Qty2'], 2)
                # self.trade_log(text=pnl, fn='pnl')
                return pnl
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def current_pnl(self):
        """To calculate live pnl of instrument"""
        fn = "current_pnl"
        try:
            if self.position is True:
                if self.trade_type is Order.buy:
                    self.pnl_trade = round((self.ltp - self.entry_price) * self.qty, 2)
                else:
                    self.pnl_trade = round((self.entry_price - self.ltp) * self.qty, 2)
                self.trade_log(text=self.pnl_trade, fn=fn)
            else:
                self.pnl_trade = round(self.pnl(), 2)
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def calc_sl_tgt(self):
        """To calculate SL & TGT and assinged to variables"""
        fn = "calc_sl_tgt"
        try:
            if self.trade_type is Order.buy:
                self.sl = Gen_Functions.round_nearest(self.entry_price * (1 - self.sl_per / 100))
                self.tgt = Gen_Functions.round_nearest(self.entry_price * (1 + self.tgt_per / 100))
            elif self.trade_type is Order.sell:
                self.sl = Gen_Functions.round_nearest(self.entry_price * (1 + self.sl_per / 100))
                self.tgt = Gen_Functions.round_nearest(self.entry_price * (1 - self.tgt_per / 100))
            else:
                self.trade_log(text=f"{self.symbol} position is {self.position}", fn=fn)
        except Exception as e:
            t = datetime.datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
            text = f"{t}:Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def resample_data(self, period):
        """Function to resample/ convert data to desired period"""
        fn = "resample_data"
        try:
            ohlc = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }
            df = self.data
            if period == '1d':
                df = df.resample('1d').apply(ohlc)
            elif period == '1h':
                df = df.resample('1h', offset='15min').apply(ohlc)
            elif period == '30min':
                df = df.resample('30min', offset='15min').apply(ohlc)
            elif period == '15min':
                df = df.resample('15min', offset='15min').apply(ohlc)
            elif period == '5min':
                df = df.resample('5min', offset='15min').apply(ohlc)
            elif period == '3min':
                df = df.resample('3min', offset='15min').apply(ohlc)
            elif period == '1min':
                df = df.resample('1min', offset='15min').apply(ohlc)
            df = df.drop(df[df.open.isnull()].index)
            self.data = df
        except Exception as e:
            t = datetime.datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
            text = f"{t}:Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)
    
    def resample_data1(period, data):
        """Function to resample/ convert data to desired period"""
        fn = "resample_data1"
        try:
            ohlc = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }
            df = data
            if period == '1d':
                df = df.resample('1d').apply(ohlc)
            elif period == '1h':
                df = df.resample('1h', offset='15min').apply(ohlc)
            elif period == '30min':
                df = df.resample('30min', offset='15min').apply(ohlc)
            elif period == '15min':
                df = df.resample('15min', offset='15min').apply(ohlc)
            elif period == '5min':
                df = df.resample('5min', offset='15min').apply(ohlc)
            elif period == '3min':
                df = df.resample('3min', offset='15min').apply(ohlc)
            elif period == '1min':
                df = df.resample('1min', offset='15min').apply(ohlc)
            df = df.drop(df[df.open.isnull()].index)
            return df
        except Exception as e:
            t = datetime.datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
            text = f"{t}:Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)

    def trade_append(self):
        """function to append trade log (trade dict) to df"""
        fn = "trade_append"
        try:
            self.trade_dict['Fund'] = self.pnl() + self.trade_dict['Fund']
            self.trade_dict['Pnl'] = self.pnl()
            new_df = pd.DataFrame(self.trade_dict, index=[0])
            self.df = pd.concat([self.df, new_df], ignore_index=True)
            self.trade_log(text="trade appended", fn="trade_append")
            # self.trade_log(text=self.df, fn="trade_append")
        except Exception as e:
            t = datetime.datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
            self.trade_log(text="error in trade append", fn="trade_append")
            text = f"{t}:Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def exit_var(self, type_of_order, price=None):
        """function to re-establish variables after getting out of a trade (exit)"""
        fn = "exit_var"
        try:
            self.trade_dict['Info'] = type_of_order.value

            if self.paper_trading is True:  # for paper trading only

                self.trade_dict['Price2'] = price
                # self.trade_log(text=f"{self.trade_dict}", fn=fn)
                self.trade_append()
                # print("inside exit var")
                self.trade_type = None
                self.position = False
                self.entry_price = None
                self.trade_log(text="executed", fn="exit_var")
            else:  # for live trading
                sleep(2)
                
                print("ensure to run update_order_details before this. (exit_var)")
                Alice_Module.update_order_details()
                if type_of_order is Order.sqoff:  # for sq off order
                    if self.order_details['exit']['status'] == self.status[4]:  # if status is complete
                        self.trade_dict['Info'] = type_of_order.value
                        self.trade_dict['Price2'] = self.order_details['exit']['price']
                        self.trade_append()
                        self.trade_type = None
                        self.position = False
                        self.entry_price = None
                        self.trade_log(text="executed", fn="exit_var")
                elif type_of_order is Order.sl:
                    if self.order_details['sl']['status'] == self.status[4]:  # if status is complete
                        self.trade_dict['Info'] = type_of_order.value
                        self.trade_dict['Price2'] = self.order_details['sl']['price']
                        self.trade_append()
                        self.trade_type = None
                        self.position = False
                        self.entry_price = None
                        self.trade_log(text="executed", fn="exit_var")
                elif type_of_order is Order.tgt:
                    if self.order_details['tgt']['status'] == self.status[4]:  # if status is complete
                        self.trade_dict['Info'] = type_of_order.value
                        self.trade_dict['Price2'] = self.order_details['tgt']['price']
                        self.trade_append()
                        self.trade_type = None
                        self.position = False
                        self.entry_price = None
                        self.trade_log(text="executed", fn="exit_var")
                elif type_of_order is Order.cancel_sl:
                    text = f"Status of SL order: {self.order_details['sl']['status']}"
                    my_logger(data_to_log=text, fn=fn, sym=self.symbol)
                    self.trade_log(text="executed", fn="exit_var")
                elif type_of_order is Order.cancel_tgt:
                    text = f"Status of TGT order: {self.order_details['tgt']['status']}"
                    my_logger(data_to_log=text, fn=fn, sym=self.symbol)
                    self.trade_log(text="executed", fn="exit_var")

        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def entry_var(self, price=None):
        """function to establish variables after getting a trade entry"""
        fn = "entry_var"
        try:
            if self.paper_trading is True:
                self.trade_dict['Price1'] = price
                self.entry_price = price
                self.calc_sl_tgt()
                self.trade_dict['Tgt'] = self.tgt
                self.trade_dict['SL'] = self.sl
                self.position = True

                self.trade_log(text="executed", fn="entry_var")
            else:
                sleep(2)
                print("ensure to run update_order_details before this. (entry_var)")
                Alice_Module.update_order_details()
                if self.order_details['entry']['status'] == self.status[4]:  # if status is complete
                    self.trade_dict['Price1'] = self.order_details['entry']['price']
                    self.entry_price = self.order_details['entry']['price']
                    self.calc_sl_tgt()
                    self.trade_dict['Tgt'] = self.tgt
                    self.trade_dict['SL'] = self.sl
                    self.position = True
                    self.trade_log(text="executed", fn="entry_var")

        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def trade_log(self, text, fn="Trade"):
        """logger for debug Trade_live"""
        try:
            my_logger(data_to_log=f"{self.symbol}: {text} |{fn}", sym="trade_log", bot=False)
        except Exception as e:
            text = f"Error: {e} | {self.symbol}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def trailing_tgt(self, modify_on=50):
        fn = "trailing_tgt"
        try:
            current_ltp = self.ltp
            modify_by = Gen_Functions.round_nearest(modify_on/2)
            if self.trade_type is Order.sell:
                difference = self.tgt - current_ltp
                # if difference is greater than modify_on var
                if difference >= modify_on:
                    self.place_order(type_of_order=Order.modify, price=(self.tgt-modify_by), modify="tgt")
            else:
                difference = current_ltp - self.tgt
                # if difference is greater than modify_on var
                if difference >= modify_on:
                    self.place_order(type_of_order=Order.modify, price=(self.tgt + modify_by), modify="tgt")
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def trailing_sl(self, loss_booked):
        fn = "trailing_sl"
        try:
            self.current_pnl()
            if self.pnl_trade > (loss_booked + 600):
                entry_price = self.order_details["entry"]["price"]
                points_to_modify = float(loss_booked // self.qty)
                if self.trade_type is Order.buy:
                    modify_sl = Gen_Functions.round_nearest(entry_price + points_to_modify)
                    self.place_order(type_of_order=Order.modify, price=modify_sl, modify="sl")
                    self.activate_sl_trailing = False
                else:
                    modify_sl = Gen_Functions.round_nearest(entry_price - points_to_modify)
                    self.place_order(type_of_order=Order.modify, price=modify_sl, modify="sl")
                    self.activate_sl_trailing = False
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

    def report(self):
        fn = 'report'
        try:
            entry_status = self.order_details['entry']['status']
            # if entry status is other than 'not initiated'
            if entry_status != self.status[0]:
                self.current_pnl()
                entry_price = self.order_details['entry']['price']
                entry_tran_type = self.order_details['entry']['tran_type']
                exit_price = self.order_details['exit']['price']
                exit_tran_type = self.order_details['exit']['tran_type']
                exit_status = self.order_details['exit']['status']
                tgt_price = self.order_details['tgt']['price']
                tgt_tran_type = self.order_details['tgt']['tran_type']
                tgt_status = self.order_details['tgt']['status']
                sl_price = self.order_details['sl']['price']
                sl_tran_type = self.order_details['sl']['tran_type']
                sl_status = self.order_details['sl']['status']
                report_data = f"{self.symbol}({self.qty})\n" \
                              f"Entry: {entry_price}({entry_tran_type})| Status: {entry_status}\n" \
                              f"Exit: {exit_price} ({exit_tran_type})| Status: {exit_status}\n" \
                              f"TGT: {tgt_price}/{self.tgt} ({tgt_tran_type}) | Status: {tgt_status} \n" \
                              f"SL: {sl_price}/{self.sl} ({sl_tran_type}) | Status: {sl_status} \n" \
                              f"PnL: {self.pnl_trade} | Ltp: {self.ltp}"
                text = report_data
                return text
            # return report_data
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            self.trade_dict = {'Inst': None, 'Entry_date': None, 'Buy_Sell1': None, 'Qty1': None, 'Price1': None,
                           'Tgt': None, 'SL': None,
                           'Exit_date': None, 'Buy_Sell2': None, 'Qty2': None, 'Price2': None, 'Pnl': 0, 'Fund': 0,
                           'Info': None}
            
    def report_paper_trade(self):
        fn = 'report_pt'
        try:
            # entry_status = self.order_details['entry']['status']
            # # if entry status is other than 'not initiated'
            # if entry_status != self.status[0]:
            self.current_pnl()
            entry_price = self.trade_dict['Price1']
            entry_tran_type = self.trade_dict['Buy_Sell1']
            exit_price = self.trade_dict['Price2']
            exit_tran_type = self.trade_dict['Buy_Sell2']
            # exit_status = self.order_details['exit']['status']
            # tgt_price = self.order_details['tgt']['price']
            # tgt_tran_type = self.order_details['tgt']['tran_type']
            # tgt_status = self.order_details['tgt']['status']
            # sl_price = self.order_details['sl']['price']
            # sl_tran_type = self.order_details['sl']['tran_type']
            # sl_status = self.order_details['sl']['status']
            report_data = f"{self.symbol}(Qty:{self.qty})\n" \
                          f"Entry: {entry_price}({entry_tran_type})\n" \
                          f"Exit: {exit_price} ({exit_tran_type})\n" \
                          f"PnL: {self.pnl_trade} | Ltp: {self.ltp}"
            text = report_data
            return text
            # return report_data
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)



    def send_order(self, type_of_order, price=None, modify=None):
        """To send order and get response as order-id and write the file pkl_obj/order.pkl"""
        fn = "send_order"
        try:
            self.trade_dict['Inst'] = self.symbol

            if type_of_order is Order.buy:  # used to take trade entry
                if self.paper_trading is False:
                    # send a buy market order (self.update_order_id_to = ["entry", "exit", "sl", "tgt"])
                    self.order_place(transaction_type=TransactionType.Buy,
                                     order_type=OrderType.Market,
                                     product_type=self.order_type,
                                     price=0.0,
                                     trigger_price=None,
                                     update_order_id_to=self.update_order_id_to[0]  # entry
                                     )
                self.trade_count += 1

                self.entry_var(price)
                text = f"Buy order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.sell:  # used to take trade entry
                self.trade_dict['Entry_date'] = current_dt
                self.trade_dict['Buy_Sell1'] = type_of_order.value
                self.trade_dict['Qty1'] = self.qty * -1
                self.trade_type = type_of_order
                if self.paper_trading is False:
                    # send a sell market order
                    self.order_place(transaction_type=TransactionType.Sell,
                                     order_type=OrderType.Market,
                                     product_type=self.order_type,
                                     price=0.0,
                                     trigger_price=None,
                                     update_order_id_to=self.update_order_id_to[0]  # entry
                                     )
                self.trade_count += 1
                self.entry_var(price)
                text = f"Sell order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.sqoff:  # used to take trade exit
                self.trade_dict['Exit_date'] = current_dt

                if self.trade_type is Order.buy:  # if buy trade is in position
                    self.trade_dict['Buy_Sell2'] = Order.sell.value
                    self.trade_dict['Qty2'] = self.qty * -1
                    if self.paper_trading is False:
                        # send a sell order
                        self.order_place(transaction_type=TransactionType.Sell,
                                         order_type=OrderType.Market,
                                         product_type=self.order_type,
                                         price=0.0,
                                         trigger_price=None,
                                         update_order_id_to=self.update_order_id_to[1]  # exit
                                         )

                else:  # if sell trade is in position
                    self.trade_dict['Buy_Sell2'] = Order.buy.value
                    self.trade_dict['Qty2'] = self.qty
                    if self.paper_trading is False:
                        # send a buy order
                        self.order_place(transaction_type=TransactionType.Buy,
                                         order_type=OrderType.Market,
                                         product_type=self.order_type,
                                         price=0.0,
                                         trigger_price=None,
                                         update_order_id_to=self.update_order_id_to[1]  # exit
                                         )

                self.exit_var(type_of_order, price)
                text = f"SqOff order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.sl:  # used to take trade sl
                self.trade_dict['Exit_date'] = current_dt
                if self.trade_type is Order.buy:
                    self.trade_dict['Buy_Sell2'] = Order.sell.value
                    self.trade_dict['Qty2'] = self.qty * -1
                    if self.paper_trading is False:
                        # send a sl sell order
                        self.order_place(transaction_type=TransactionType.Sell,
                                         order_type=OrderType.StopLossLimit,
                                         product_type=self.order_type,
                                         price=self.sl - 5,
                                         trigger_price=self.sl,
                                         update_order_id_to=self.update_order_id_to[2]
                                         # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                                         )

                else:
                    self.trade_dict['Buy_Sell2'] = Order.buy.value
                    self.trade_dict['Qty2'] = self.qty
                    if self.paper_trading is False:
                        # send a sl buy order
                        self.order_place(transaction_type=TransactionType.Buy,
                                         order_type=OrderType.StopLossLimit,
                                         product_type=self.order_type,
                                         price=self.sl + 5,
                                         trigger_price=self.sl,
                                         update_order_id_to=self.update_order_id_to[2]
                                         # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                                         )
                self.exit_var(type_of_order, price)
                text = f"SL order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.tgt:  # used to take trade tgt
                self.trade_dict['Exit_date'] = current_dt
                if self.trade_type is Order.buy:
                    self.trade_dict['Buy_Sell2'] = Order.sell.value
                    self.trade_dict['Qty2'] = self.qty * -1
                    if self.paper_trading is False:
                        # send a sell sl order
                        self.order_place(transaction_type=TransactionType.Sell,
                                         order_type=OrderType.StopLossLimit,
                                         product_type=self.order_type,
                                         price=self.tgt - 5,
                                         trigger_price=self.tgt,
                                         update_order_id_to=self.update_order_id_to[3]
                                         # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                                         )
                else:
                    self.trade_dict['Buy_Sell2'] = Order.buy.value
                    self.trade_dict['Qty2'] = self.qty
                    if self.paper_trading is False:
                        # send a buy market order
                        self.order_place(transaction_type=TransactionType.Buy,
                                         order_type=OrderType.StopLossLimit,
                                         product_type=self.order_type,
                                         price=self.tgt + 5,
                                         trigger_price=self.tgt,
                                         update_order_id_to=self.update_order_id_to[3]
                                         # self.update_order_id_to = ["entry", "exit", "sl", "tgt"]
                                         )
                self.exit_var(type_of_order, price)
                text = f"Tgt order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.modify:
                if modify == 'sl':
                    self.sl = price
                    if self.paper_trading is False:
                        # send a modify sl order
                        if self.trade_type is Order.sell:
                            self.order_modify(transaction_type=TransactionType.Buy,
                                              order_id=self.order_details['sl']['order_id'],
                                              order_type=OrderType.StopLossLimit,
                                              product_type=self.order_type,
                                              price=self.sl + 5,
                                              trigger_price=self.sl
                                              )
                        else:
                            self.order_modify(transaction_type=TransactionType.Sell,
                                              order_id=self.order_details['sl']['order_id'],
                                              order_type=OrderType.StopLossLimit,
                                              product_type=self.order_type,
                                              price=self.sl - 5,
                                              trigger_price=self.sl
                                              )

                elif modify == 'tgt':
                    self.tgt = price
                    if self.paper_trading is False:
                        # send a modify tgt order
                        if self.trade_type is Order.sell:
                            self.order_modify(transaction_type=TransactionType.Buy,
                                              order_id=self.order_details['tgt']['order_id'],
                                              order_type=OrderType.StopLossLimit,
                                              product_type=self.order_type,
                                              price=self.tgt + 5,
                                              trigger_price=self.tgt
                                              )
                        else:
                            self.order_modify(transaction_type=TransactionType.Sell,
                                              order_id=self.order_details['tgt']['order_id'],
                                              order_type=OrderType.StopLossLimit,
                                              product_type=self.order_type,
                                              price=self.tgt - 5,
                                              trigger_price=self.tgt
                                              )
                text = f"Modify order sent at {price}."
                my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)

            elif type_of_order is Order.cancel_sl:
                if self.paper_trading is False and self.order_details['sl']['status'] in self.open_orders:
                    self.order_cancel(order_id=self.order_details['sl']['order_id'])
                    self.exit_var(type_of_order, price)
                    self.trade_log(f"{self.dt} cancel sl order sent")

            elif type_of_order is Order.cancel_tgt:
                if self.paper_trading is False and self.order_details['tgt']['status'] in self.open_orders:
                    self.order_cancel(order_id=self.order_details['tgt']['order_id'])
                    self.exit_var(type_of_order, price)
                    self.trade_log(f"{self.dt} cancel tgt order sent")
        except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, sym=self.symbol, bot=True)