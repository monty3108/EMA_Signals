# Module for AliceBlue API Functions
# Ver 1
import sys
from logging import exception

from pya3 import *
import os
import json
import Gen_Functions
from time import sleep
from Logger_Module import my_logger
import Trade_Live
import numpy as np
import config
import datetime
# Setting Up logger for logging
from My_Logger import setup_logger, LogLevel
logger = setup_logger(logger_name="Alice Module", log_level=LogLevel.INFO, log_to_console=config.print_logger)

trade_class_list = []
subscribe_list = None
unsubscribe_list = []  # list of instrument for unsubscribe updates

# symbol constants
NB = 'NIFTY BANK'  # for Spot Index
BN = 'BANKNIFTY'  # for NFO
NIFTY = 'NIFTY 50'
VIX = 'INDIA VIX'
EXCHANGE = 'NSE'

# websockets variables
LTP = 0
socket_opened = False
subscribe_flag = False
alice = None


def update_df(file_name="result.csv"):
    """Make journal of Entry and Exit Trades as per entry & exit orders given by strategy. And append to CSV file."""
    fn = "update_df"
    global trade_class_list
    result = pd.DataFrame()
    try:
        for trade_class in trade_class_list:
            result = pd.concat([result, trade_class.df], ignore_index=True)
        if len(result) > 0:
            result = result.sort_values(by=['Entry_date'], ignore_index=True)
            print(result)
            result_file_exist = os.path.exists(file_name)
            if result_file_exist:
                result.to_csv(file_name, mode='a', index=False, header=False)
                my_logger(data_to_log=f"Result appended to: {file_name}.",
                          fn=fn,
                          bot=False)
            else:
                result.to_csv(file_name, index=False)
                my_logger(data_to_log=f"New file created: {file_name}.",
                          fn=fn,
                          bot=False)

            df = pd.read_csv(file_name)
            df['Fund'] = np.cumsum(df['Pnl'])
            df.to_csv(file_name, index=False)
        else:
            my_logger(data_to_log="No Trades to update", fn=fn, bot=True)
        
    except Exception as e:
        text = f"Error: {e}."
        my_logger(data_to_log=text, fn=fn, bot=True)

def credentials():
    fn = 'credentials'
    try:
        # Config
        with open("credentials.txt", mode='r') as f:
            credentials_data = f.readlines()
        for d in range(6):
            credentials_data[d] = credentials_data[d].strip()
        logger.info("credentials imported from text file successfully.")
        return credentials_data
    except Exception as e:
        text = f"Error: {e}."
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.error(text)

def get_session_id():
    fn = 'get_session_id'
    global alice
    try:
        # session[0]: date, session[1]: session_id
        if Gen_Functions.file_exist(config.path_session):
            session = Gen_Functions.read_pkl(file_path='pkl_obj/session.pkl')
            if session[0] == Gen_Functions.today_date():
                config.alice = session[1]
                alice = config.alice
                txt = f'Session id retrieved from session.pkl for date: {Gen_Functions.today_date()}'
                logger.info(txt)
            else:
                session_id_generate()
        else:
            logger.info(f"file {config.path_session} is not exist. Creating new session.")
            session_id_generate()
    except Exception as e:
        logger.exception(e)
        my_logger(data_to_log=e, fn=fn, bot=True)
        session_id_generate()

# To generate new session
def session_id_generate(download_contract=1):
    fn = 'session_id_generate'
    data = credentials()
    global alice
    try:
        logger.debug(f'Before calling session generation alice: {alice} ')
        session = Aliceblue(user_id=data[0], api_key=data[5])
        response = session.get_session_id()
        # logger.info(response)
        if response['stat'] == 'Ok':
            session_pkl = [Gen_Functions.today_date(), session]
            session_id = response['sessionID']
            text = "Session Id generated"
            my_logger(data_to_log=text, fn=fn, bot=True)
            logger.info(text)
            logger.info(f'alice obj: {session}')
            Gen_Functions.create_dir(['pkl_obj'])
            Gen_Functions.write_pkl(obj=session_pkl, file_path='pkl_obj/session.pkl')    
            config.alice = session
            alice = config.alice
            if download_contract == 1:
                logger.info('Downloading master contract......')
                # my_logger(data_to_log='Downloading master contract......', fn=fn, bot=False)
                get_master_contract()
                logger.info('Master contract Downloaded.')
                my_logger(data_to_log='Master contract Downloaded.', fn=fn, bot=False)
            
        else:
            text = f"Response: {response}. Calling function again after 15 secs."
            my_logger(data_to_log=text, fn=fn, bot=False)
            logger.critical(text) 
            text1 = f"Stat Not Ok. Calling function again after 15 secs."
            my_logger(data_to_log=text1, fn=fn, bot=True)
            sleep(15)
            session_id_generate()
    except Exception as e:
        text = f"Error: {e}. Calling function again after 1 min. Log In your account using TOTP."
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.error(text)
        logger.exception(e)
        sleep(60)
        my_logger(data_to_log="Function recalled.", fn=fn, bot=False)
        logger.info('Function Recalled.') 
        session_id_generate()

def get_master_contract():
    global alice
    alice.get_contract_master("NFO")
    alice.get_contract_master("NSE")
    alice.get_contract_master("INDICES")
    # alice.get_contract_master("BSE")
    # alice.get_contract_master("CDS")
    # alice.get_contract_master("BFO")
    # alice.get_contract_master("MCX")

def socket_open():  # Socket open callback function
    global alice
    fn = 'socket_open'
    t1 = "Socket Opened"
    t2 = "Connected"
    my_logger(data_to_log=t1, fn=fn, bot=False)
    my_logger(data_to_log=t2, fn=fn, bot=True)
    global socket_opened, subscribe_list
    socket_opened = True
    if subscribe_flag:  # This is used to resubscribe the script when reconnect the socket.
        subscribe()
        print(
            "-----------------------Re-subscribed------------------------------"
        )

def socket_close():  # On Socket close this callback function will trigger
    fn = 'socket_close'
    global socket_opened, LTP
    socket_opened = False
    LTP = 0
    print("Socket Closed")
    my_logger(data_to_log="Socket Closed", fn=fn, bot=True)

def socket_error(
        message
):  # Socket Error Message will receive in this callback function
    fn = 'socket_error'
    global LTP
    LTP = 0
    # text = f"Socket Error : {message}"
    # print(f"{text}")
    # my_logger(data_to_log=text, fn=fn, bot=False)


def feed_data(
        message):  # Socket feed data will receive in this callback function
    fn = 'feed_data'
    global LTP, subscribe_flag
    feed_message = json.loads(message)
    if feed_message["t"] == "ck":
        print("Connection Acknowledgement status :%s (Websocket Connected)" %
              feed_message["s"])
        subscribe_flag = True
        print("subscribe_flag :", subscribe_flag)
        print(
            "-------------------------------------------------------------------------------"
        )
        pass
    elif feed_message["t"] == "tk":
        print("Token Acknowledgement status :%s " % feed_message)
        scrip = feed_message['tk']
        text = f"Token Acknowledgement status : {scrip}"
        my_logger(data_to_log=text, fn=fn, bot=False)
        # logger.info(text)
        print(
            "-------------------------------------------------------------------------------"
        )
        pass
    else:
        # print("Feed :", feed_message)
        LTP = feed_message[
            'lp'] if 'lp' in feed_message else LTP  # If LTP in the response it will store in LTP variable
        live_ltp(feed_message
                 )  # assign ltp to corresponding scrip via live_ltp function


def live_ltp(feed_message):
    global trade_class_list
    # print(feed_message)
    # ts = datetime.datetime.fromtimestamp(int(feed_message['ft'])).strftime('%Y-%m-%d %H:%M:%S')
    for trade_class in trade_class_list:
        if trade_class.token == int(feed_message['tk']):
            trade_class.ltp = float(feed_message['lp']) if 'lp' in feed_message else trade_class.ltp
            trade_class.ft = feed_message['ft'] if 'ft' in feed_message else trade_class.ft
            # print(type(feed_message['ft']))
            if trade_class.feed:
                # trade_class.ltp_time = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                # feed_list = {'ft': feed_message['ft'], 'ltp': trade_class.ltp}
                feed_list = {'ft': feed_message['ft'], 'ltp': trade_class.ltp}
                # feed_list = feed_message
                df = pd.DataFrame(feed_list, index=[0])
                file_name = f"{trade_class.token}.csv"
                if trade_class.csv_file_existed:
                    df.to_csv(file_name, mode='a', index=False, header=False)
                else:
                    df.to_csv(file_name, mode='a', index=False)
                    trade_class.csv_file_existed = True
    # for item in class_list:
    #     print(f"{item.symbol}: {item.ltp}")


def ltp_update():
    fn = 'ltp_update'
    global trade_class_list
    i = 1
    scrips=[]
    logger.info('ltp_update started')
    my_logger(data_to_log="ltp_update started", fn=fn, bot=True)
    for trade_class in trade_class_list:
        if trade_class.instrument is not None:
            scrips.append(trade_class)

    for item in scrips:
        while item.ltp is None:
            i += 1
            if i > 60:
                logger.info('1 min passed. Breaking') 
                print('breaking ltp update loop') 
                break
            sleep(1)
            pass
        text = f"{item.symbol} LTP: {item.ltp}"
        logger.info(text)
        my_logger(data_to_log=text, fn=fn, bot=False)
    my_logger(data_to_log="ltp_update exited", fn=fn, bot=True)
    logger.info('ltp_update exited') 
    # my_logger(data_to_log="Scrips subscribed successfully", fn=fn, bot=True)


def report_send():
    global trade_class_list
    text = []
    fn = 'report_send'
    try:
        update_order_details()
        for inst in range(2):
            text.append(
                f"{trade_class_list[inst].symbol}(ltp): {trade_class_list[inst].ltp}\n"
            )
        mtm = 0
        for item in trade_class_list:
            report_return = item.report()
            if report_return is not None:
                text.append(f"{report_return}\n")
                mtm = mtm + item.pnl_trade
        text.append(f"MTM: {mtm}")
        report = ''
        for data in text:
            report += data + '\n'
        my_logger(data_to_log=report, fn=fn, bot=True)
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        
def report_send_pt(): # used for paper trading report
    global trade_class_list
    text = []
    fn = 'report_send_pt'
    try:
        # update_order_details()
        # for inst in range(2):
        #     text.append(
        #         f"{trade_class_list[inst].symbol}(ltp): {trade_class_list[inst].ltp}\n"
        #     )
        mtm = 0
        for item in trade_class_list:
            report_return = item.report_paper_trade()
            if report_return is not None:
                text.append(f"{report_return}\n")
                mtm = mtm + item.pnl_trade
        text.append(f"MTM: {mtm}")
        report = ''
        for data in text:
            report += data + '\n'
        my_logger(data_to_log=report, fn=fn, bot=True)
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)


def gross_pnl(trade_list):  # input: list of variable of AliceTrade
    gross_pnl = 0
    for item in trade_class_list:
        item.pnl_calc()
        gross_pnl = gross_pnl + item.pnl
    return gross_pnl


""" Web Socket """


# ----------------------------------------------------------------------------------------
#  *************** Web Socket ****************************************
# ----------------------------------------------------------------------------------------
def alice_websocket(depth=False):
    print("Websocket Module Starting....")
    global alice
    alice.start_websocket(socket_open_callback=socket_open,
                          socket_close_callback=socket_close,
                          socket_error_callback=socket_error,
                          subscription_callback=feed_data,
                          run_in_background=True,
                          market_depth=depth)

    while not socket_opened:
        fn = 'Web Socket'
        my_logger(data_to_log="Socket is closed", fn=fn, bot=False)
        print("socket is closed")
        sleep(15)
        pass


def subscribe():
    fn = "subscribe"
    global alice, trade_class_list
    try:
        subscribe_list_inst = []  # list of instrument for updates
        for inst in trade_class_list:
            if inst.instrument is not None:
                subscribe_list_inst.append(inst.instrument)
        alice.subscribe(subscribe_list_inst)
        text = f"Feed subscribed for: \n{subscribe_list_inst}"
        # logger.info(text)
        my_logger(data_to_log=text, fn=fn, bot=False)
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)

def unsubscribe():
    """ Func to unsubscribe list of scripts"""
    fn = "unsubscribe"
    global alice, trade_class_list
    try:
        unsubscribe_list_inst = []  # list of instrument for updates
        for inst in trade_class_list:
            if inst.instrument is not None:
                unsubscribe_list_inst.append(inst.instrument)
        alice.unsubscribe(unsubscribe_list_inst)
        text = f"Feed unsubscribed for: {unsubscribe_list_inst}"
        # logger.info(text)
        my_logger(data_to_log=text, fn=fn, bot=False)
        logger.info(text)
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.error(text)


def update_order_details():
    """Func for get order update and append to pending = [], completed = [], and pending_order_id = []"""
    fn = "update_order_details"
    try:
        global alice, trade_class_list
        response = alice.get_order_history('')

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
                id_status[response[i]['Nstordno']] = {
                    "stat": response[i]['stat'],
                    "order_status": response[i]['Status'],
                    "trading_symbol": response[i]['Trsym'],
                    "average_price": float(response[i]['Avgprc']),
                    "trigger_price": response[i]['Trgprc'],
                    "product": response[i]['Pcode'],
                    "price": response[i]['Prc'],
                    "transaction_type": response[i]['Trantype']
                }
                if response[i]['Status'] == 'open' or response[i][
                        'Status'] == 'trigger pending':
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
                order_status = orders['data'][
                    'id_status']  # assign id_status to order_status

                for trade_class in trade_class_list:
                    for items in trade_class.order_details:
                        status = trade_class.order_details[items]['status']
                        # if status is other than Not Initiated or 'complete'
                        if status != trade_class.status[
                                0] or status != trade_class.status[4]:
                            # assigning order_id to variable from order detail
                            order_id = trade_class.order_details[items][
                                'order_id']

                            if order_id in order_status:  # updating status & avg price
                                trade_class.order_details[items][
                                    'status'] = order_status[order_id][
                                        'order_status']
                                trade_class.order_details[items][
                                    'price'] = order_status[order_id][
                                        'average_price']
                                trade_class.order_details[items]['tran_type'] = \
                                    order_status[order_id]['transaction_type']

                    # text = trade_class.order_details
                    # my_logger(data_to_log=text, fn=fn, bot=False)
                    
            
            # update_positions() # to update position detail if completed.
            return old_response
        else:
            return response
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)

            
def open_net_position(Net_position):
    """Func to filter open net positions"""
    fn = "open_net_position"
    try:
        open_net_position = [data for data in Net_position if data['Netqty'] != '0']
        return open_net_position
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)

def close_net_position(Net_position):
    """Func to filter close net positions"""
    fn = "close_net_position"
    try:
        close_net_position = [data for data in Net_position if data['Netqty'] == '0']
        return close_net_position
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)

def check_net_position():
    """Func to retrieve open and close net positions"""
    fn = "check_net_position"
    global alice
    try:
        get_netwise_positions = alice.get_netwise_positions()
        if type(get_netwise_positions) is list:
            # print(get_netwise_positions)
            open_position = open_net_position(get_netwise_positions)
            close_position = close_net_position(get_netwise_positions)
            response = {
                        "stat": "Ok",
                        "message": "",
                        "data": {
                            "open_net_position": open_position,
                            "close_net_position": close_position
                                }
            }
        else:
            response = get_netwise_positions
        return response 
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)

def update_positions():
    """ update position value for exited positions """
    fn = "update_positions"
    global trade_class_list
    try:
        net_position = check_net_position() # checking for net positions
        if net_position['stat'] == 'Ok': # if net position report has some data
            closed_position = net_position["data"]["close_net_position"] # list of closed positions

            for trade_class in trade_class_list: # iterating over all assigned trade class
                for items in closed_position: # iterating over all closed positions
                    if trade_class.token == int(items['Token']): # if token matched with closed position
                        if trade_class.position is True: # posn is closed but position var is not updated
                            my_logger(data_to_log="Position var is True but position is closed. Variable updating....", fn=fn, sym=trade_class.symbol, bot=True)
                            trade_class.position = False
                            list_of_exit_orders = [Trade_Live.Order.sqoff, Trade_Live.Order.sl, Trade_Live.Order.tgt]
                            # list_of_exit_orders = [Order.sqoff, Order.sl, Order.tgt]
                            for orders in list_of_exit_orders:
                                trade_class.exit_var(orders)
                            my_logger(data_to_log="Position closed. Check & cancel pending order of this scrip.", fn=fn, sym=trade_class.symbol, bot=True)
        elif net_position['stat'] == 'Not_Ok':
            my_logger(data_to_log=net_position, fn=fn, bot=False)
            
        else:
            my_logger(data_to_log=f"error: {net_position}", fn=fn, bot=True)
    except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)  

def square_off_open_orders():  # pending to include square off of positions
    fn = 'square_off_open_orders'
    """ Cancellation of all open orders including trigger pending orders """
    try:
        global trade_class_list
        response = update_order_details()
        trade_class_order_ids = []
        for trade_class in trade_class_list:
            for item in trade_class.order_details.values():
                if item['status'] in trade_class.open_orders:
                    trade_class_order_ids.append(item['order_id'])
        print(f"These Order Ids will be sent for cancellation: {trade_class_order_ids}")
        for ids in trade_class_order_ids:
            self.order_cancel(order_id=ids)
        text = f"cancel order sent for: {trade_class_order_ids}"
        my_logger(data_to_log=text, fn=fn, bot=True)
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)

def sqoff_strategy():
    fn = "sqoff_strategy"
    global trade_class_list
    try:      
        # cancel all pending orders
        square_off_open_orders()
        # update positions
        update_positions()
        net_position = check_net_position() # checking for net positions
        if net_position['stat'] == 'Ok': # if net position report has some data
            open_position = net_position["data"]["open_net_position"] # list of open positions
            for trade_class in trade_class_list: # iterating over all assigned trade class
                for items in open_position: # iterating over all open positions
                    if trade_class.token == int(items['Token']): # if token matched with closed position
                        if trade_class.position is True: # posn is closed but position var is not updated
                            trade_class.place_order(type_of_order=Order.sqoff)
            my_logger(data_to_log="SqOff order sent for open positions.", fn=fn, bot=True)
        
        elif net_position['stat'] == 'Not_Ok':
            my_logger(data_to_log=net_position, fn=fn, bot=False)
            
        else:
            my_logger(data_to_log=f"error: {net_position}", fn=fn, bot=True)
    except Exception as e:
            text = f"Error: {e}"
            my_logger(data_to_log=text, fn=fn, bot=True)            
        
def update_trade_journal():
    """Update csv as per Trade Log from AB"""
    global alice
    fn = "update_trade_journal"
    try:
        file_name = "trade_journal.csv"
        today_trades = alice.get_trade_book()
        if type(today_trades) is list:
            trade_book = Gen_Functions.reverse_list(today_trades)
            trade_log = []
            var_list = [
                        "Tsym", "Exchtime", "Trantype", 'Qty', "AvgPrice", "Exchtime",
                        'Trantype', 'Qty', "AvgPrice"
                    ]

            for trades in trade_book:
                inst = trades[var_list[0]]
                entry_date = datetime.datetime.strptime(trades[var_list[1]],
                                                        "%d-%b-%Y %H:%M:%S")
                b_s = trades[var_list[2]]
                price = float(trades[var_list[4]])
                if b_s == 'S':
                    qty = trades[var_list[3]] * -1
                    amount = round(price * trades[var_list[3]],2)
                else:
                    qty = trades[var_list[3]]
                    amount = round(price * trades[var_list[3]] * -1,2)

                trade_dict = {
                            'Inst': inst,
                            'Date_time': entry_date,
                            'Buy_Sell': b_s,
                            'Qty': qty,
                            'Price': price,
                            'Amount': amount,
                            'PnL': None
                        }
                trade_log.append(trade_dict)

            df = pd.DataFrame(trade_log)

            result_file_exist = os.path.exists(file_name)
            if result_file_exist:
                df.to_csv(file_name, mode='a', index=False, header=False)
                my_logger(data_to_log=f"Trade journal appended: {file_name}.",
                          fn=fn,
                          bot=False)
            else:
                df.to_csv(file_name, index=False)
                my_logger(data_to_log=f"Trade journal created: {file_name}.",
                          fn=fn,
                          bot=False)

            df = pd.read_csv(file_name)
            df['PnL'] = np.cumsum(df['Amount'])
            # df['Fund'] = np.cumsum(df['Pnl'])
            df.to_csv(file_name, index=False)
        else:
            my_logger(data_to_log=today_trades, fn=fn, bot=True)
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        
def update_balance():
    """Updade csv as per balance query from AB"""
    fn="update_balance"
    global alice
    try:
        bal = alice.get_balance()
        today_bal = {
        'Bal_date': str(datetime.datetime.now().date()), 
        'segment' : bal[0]['segment'],
        'credits' : bal[0]['credits'],
        'mtm': bal[0]['realizedMtomPrsnt'], 
        'charges': bal[0]['brokeragePrsnt'], 
        'debits' : bal[0]['debits'], 
        'net' : bal[0]['net'] 
        }
        file_name = "balance.csv"
        df = pd.DataFrame(today_bal, index=[0])
        result_file_exist = os.path.exists(file_name)
        print(today_bal)
        if result_file_exist:
            df.to_csv(file_name, mode='a', index=False, header=False)
            my_logger(data_to_log=f"Bal journal appended: {file_name}.", fn=fn, bot=False)
        else:
            df.to_csv(file_name, index=False)
            my_logger(data_to_log=f"Bal journal created: {file_name}.", fn=fn, bot=True)
        
        df = pd.read_csv(file_name)
        df['PnL'] = np.cumsum(df['mtm'])
        df['R_charges'] = np.cumsum(df['charges'])
        # df['Fund'] = np.cumsum(df['Pnl'])
        df.to_csv(file_name, index=False)
        print(df)
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
       

def file_writer(df, file_path):
    """ Func to write pandas df at the given path""" 
    log_dir = 'logs' 
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    if os.path.exists(file_path):
        # Append DataFrame to the existing file
        df.to_csv(file_path, mode='a', header=False, index=False)

    else:
        # Create a new file and write the DataFrame
        df.to_csv(file_path, index=False)


def log_trade_book() :
    """Func to log Trade Book on Day End in dir logs/trade_logs.csv"""
    try: 
        global alice
        all_trade_logs = alice.get_trade_book()
        logger.debug(all_trade_logs)
        print('Trade Logs Response:') 
        print(json.dumps(all_trade_logs, indent=4)) 
        log_is_list = False
        if isinstance(all_trade_logs, list):
            logger.info('Trade log is a list. Continue process.')
            log_is_list = True
        else:
            logger.warning(f"Trade log is not a list: {all_trade_logs}" )
        
        if log_is_list:
            trade_log_list = []
            
            for log in all_trade_logs:
                qty = int(log['Filledqty'])
                avg_price = float(log['Price'])
                tran_type =  log['Trantype']
                if tran_type == 'S' :
                    amount = round(qty*avg_price,2)
                else:
                    amount = round((qty * avg_price * -1),2)
                trade_log = {
                "Exchtime": log['Exchtime'],
                "Tsym": log['Tsym'] , 
                "Trantype": tran_type, 
                "AvgPrice": avg_price, 
                "Qty": qty , 
                "Amount": amount, 
                "Profit" : "" 
                }
                trade_log_list.append(trade_log)
            
            logger.debug('all trade logs appended to trade_log') 
            # Converting trade logs to df
            df = pd.DataFrame(trade_log_list)
            # Converting str time to datetime
            df['Exchtime'] = pd.to_datetime(df['Exchtime'], format='%d-%m-%Y %H:%M:%S' )
            file_path = config.path_trade_log # path for csv file
            file_writer(df, file_path) # For writing a new file else append
            
            logger.info('Adding Cum Sum in trade_log.csv')
            df1 = pd.read_csv(file_path)
            df1['Exchtime'] = pd.to_datetime(df1['Exchtime'], format='%Y-%m-%d %H:%M:%S' )
            df1 = df1.sort_values(by='Exchtime') # Sorting rows
            logger.debug("Values sorted by exchtime") 
            df1['Profit'] = round(df1['Amount'].cumsum(),2)
            df1.to_csv(file_path, index=False)  # rewriting existing file
            logger.info('trade_log updated & exiting') 
            
    except Exception as e:
        text = f"Error: {e}"
        logger.error(text)
      
    
def log_all_logs() :
    get_netwise_positions = alice.get_netwise_positions()
    
    get_holding_positions = alice.get_holding_positions()
    
    get_daywise_positions = alice.get_daywise_positions()
    
    get_order_history = alice.get_order_history('')
    
    get_balance =alice.get_balance()
    
    get_profile=alice.get_profile()
    
    get_trade_book = alice.get_trade_book()
    
    response = [get_netwise_positions, get_holding_positions, get_daywise_positions, get_order_history, get_balance, get_profile, get_trade_book]
    name = ['get_netwise_positions', 'get_holding_positions', 'get_daywise_positions', 'get_order_history', 'get_balance', 'get_profile', 'get_trade_book']
    
    # write to json file
    i=0
    for res in response:
        write_name = f"logs/{name[i]}.json"
        write_var = res
    
        with open(write_name, "w") as f:
            json.dump(write_var, f, indent=4)
            logger.info(f'{write_name} written') 
        
        i += 1
       
     
def log_balance() :
    """Func to log balance on Day Start/End in dir logs/balance.csv"""
    try: 
        global alice
        path = config.path_balance
        logs = alice.get_balance()
        logger.debug(f'get_balance response: {logs} ')
        #print(json.dumps(logs, indent=4)) 
        log_is_list = False
        if isinstance(logs, list):
            logger.info(f'log is a list. Continue process.')
            log_is_list = True
        else:
            logger.warning(f"log is not a list: {logs}" )
        
        if log_is_list:
            log_list = []
            
            for log in logs:
                today_date = Gen_Functions.today_date()
                print(today_date)
                cashmargin = float(log['cashmarginavailable'])
                marginused =  log['cncMarginUsed']
                
                final_log = {
                "Date": today_date, 
                "Margin": cashmargin , 
                "Margin_Used": marginused
                }
                log_list.append(final_log)
            logger.debug('all logs appended to log_list ') 
            #print(json.dumps(trade_log_list, indent=4))
            df = pd.DataFrame(log_list)
            file_path = path
            file_writer(df, file_path)
            logger.info(f'file: {path} written')
    except Exception as e:
        text = f"Error: {e}"
        logger.exception(text)



def log_strategy_book():
    """log all completed orders for all order tags"""
    path = config.path_order_history

    with open(path, 'r') as file:
        order_history = json.load(file)

    trade_log_list = []
    if isinstance(order_history, list):
        logger.info(f'{path} is a list. Continue process.')
        for order in order_history:
            if order["Status"] == "complete":
                qty = int(order['Fillshares'])
                avg_price = float(order['Avgprc'])
                tran_type = order['Trantype']

                if tran_type == 'S':
                    amount = round(qty * avg_price, 2)
                else:
                    amount = round((qty * avg_price * -1), 2)

                if order['ordersource'] == 'NA':
                    tag = 'manual'
                else:
                    tag = order['ordersource']

                trade_log = {
                    "tag" : tag,
                    "Exchtime": order['OrderedTime'],
                    "Tsym": order['Trsym'],
                    "Trantype": tran_type,
                    "AvgPrice": avg_price,
                    "Qty": qty,
                    "Amount": amount,
                    "Profit": ""
                }
                trade_log_list.append(trade_log)

        # Converting trade logs to df
        df = pd.DataFrame(trade_log_list)
        # Converting str time to datetime
        df['Exchtime'] = pd.to_datetime(df['Exchtime'], format='%d/%m/%Y %H:%M:%S')
        file_path = config.path_strategy_log  # path for csv file
        file_writer(df, file_path)  # For writing a new file else append

        logger.info(f'Adding Cum Sum in {file_path}')
        df1 = pd.read_csv(file_path)
        df1['Exchtime'] = pd.to_datetime(df1['Exchtime'], format='%Y-%m-%d %H:%M:%S')
        df1 = df1.sort_values(by='Exchtime')  # Sorting rows
        logger.debug("Values sorted by exchtime")
        df1['Profit'] = round(df1['Amount'].cumsum(), 2)
        df1.to_csv(file_path, index=False)  # rewriting existing file
        logger.info(f'{file_path} updated & exiting')
        write_separate_strategies()
    else:
        logger.info(f'{path} is not a list. Aborting process.')


def write_separate_strategies():
    """calling from log_strategy_book to write separate tags csv"""
    df = pd.read_csv(config.path_strategy_log)
    all_tags = []

    for index in df.index:
        tag = df.iloc[index]['tag']
        if tag in all_tags:
            pass
        else:
            all_tags.append(tag)

    for tag in all_tags:
        rows = []
        for index in df.index:
            if tag == df.iloc[index]['tag']:
                rows.append(df.iloc[index])
        tag_df = pd.DataFrame(rows)
        tag_df_path = f"logs/{tag}.csv"
        tag_df.to_csv(tag_df_path, index=False)

    for tag in all_tags:
        tag_df_path = f"logs/{tag}.csv"
        df1 = pd.read_csv(tag_df_path)
        df1['Exchtime'] = pd.to_datetime(df1['Exchtime'], format='%Y-%m-%d %H:%M:%S')
        df1 = df1.sort_values(by='Exchtime')  # Sorting rows
        logger.debug("Values sorted by exchtime")
        df1['Profit'] = round(df1['Amount'].cumsum(), 2)
        df1.to_csv(tag_df_path, index=False)  # rewriting existing file
        logger.info(f'{tag_df_path} updated & exiting')


def position_report(add_to_report: list = None):
    """Func to return report of all active positions"""
    fn = 'position_report'
    try:
        global alice
        report = []
        r_dict = {}
        pnl = 0
        response_is_list = False
        response = alice.get_netwise_positions()
        if isinstance(response, list):
            response_is_list = True
        else:
            logger.info(response)
            response = ["No open positions."]
            if add_to_report:
                response.append(add_to_report)
            return json.dumps(response, indent=4)

        for res in response:
            r_dict = {
                'Sym': res['Tsym'],
                'Qty': int(res['Netqty']),
                'Buy': float(res['NetBuyavgprc']),
                'Sell': float(res['NetSellavgprc']),
                'Ltp': float(res['LTP']),
                'PnL': float(res['realisedprofitloss']),
                'Mtm': float(res['unrealisedprofitloss'])
            }
            pnl += float(res['realisedprofitloss']) + float(res['unrealisedprofitloss'])
            report.append(r_dict)
        r_dict = {'Total PnL': pnl}
        report.append(r_dict)
        if add_to_report:
            report += add_to_report
        return json.dumps(report, indent=4)
    except Exception as e:
        text = f"{e}"
        my_logger(text, fn=fn, bot=True)
        logger.exception(text)


def history_resample(inst, days_back: int = 1000, resample_period: str = '1d', indices=False, skip_current_day=False):
    """"func to download the history and resample to desired timeframe and make ready to use with indicators.
    session_id and config.alice must be declared before calling this func.
    Args:
        inst: alice instrument.
        days_back (int): days of history to download.
        resample_period (str): 1d, 1h, 30min, 15min, 5min, 3min, 1min

    Returns:
        df: resampled history with datetime column as index."""
    try:
        from_date = datetime.datetime.now().replace(hour=9, minute=14, second=0) - datetime.timedelta(days=days_back)
        to_date = datetime.datetime.now()
        # to_date = datetime.datetime.now().replace(hour=15, minute=30, second=0) - datetime.timedelta(days=1)

        if resample_period == '1d':
            # if resampled time period is 1d, It will download Daily history directly from server and add last\
            # row of raw_data as in Daily time frame it is not downloading last day data
            daily_history = config.alice.get_historical(instrument=inst, from_datetime=from_date, to_datetime=to_date,
                                                   interval="D", indices=indices)
            daily_history['datetime'] = pd.to_datetime(daily_history['datetime'])  # changing [datetime] column to type datetime
            daily_history = daily_history.set_index('datetime')
            if not skip_current_day:
                # for 1min data
                from_date1 = datetime.datetime.now().replace(hour=9, minute=14, second=0) - datetime.timedelta(days=7)
                to_date1 = datetime.datetime.now()
                raw_data1 = config.alice.get_historical(instrument=inst, from_datetime=from_date1, to_datetime=to_date1,
                                                       interval="1", indices=indices)
                raw_data1['datetime'] = pd.to_datetime(raw_data1['datetime'])  # changing [datetime] column to datetime
                raw_data1 = raw_data1.set_index('datetime')  # setting [datetime] column to index
                # print(raw_data)
                df1 = raw_data1
    
                # Resampling data
                ohlc = {
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }
                period = resample_period
                if period == '1d':
                    df1 = df1.resample('1d').apply(ohlc)
                daily_history = pd.concat([daily_history, df1.iloc[[-1]]])
            return daily_history
        else:
            raw_data = config.alice.get_historical(instrument=inst, from_datetime=from_date, to_datetime=to_date,
                                                   interval="1", indices=indices)
            raw_data['datetime'] = pd.to_datetime(raw_data['datetime'])  # changing [datetime] column to datetime
            raw_data = raw_data.set_index('datetime')  # setting [datetime] column to index
            # print(raw_data)
            df = raw_data
            period = resample_period
            # Resampling data
            ohlc = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }

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
            # dropping null
            df = df.drop(df[df.open.isnull()].index)
            logger.info(f'Resampled data of {inst.symbol} to period {period}. Data Range: {from_date} to {to_date}')
            # returning the resampled data
            return df

    except Exception as e:
        logger.exception(e)

