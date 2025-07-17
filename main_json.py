""" Module for managing:
    Trade logs
    Positions
    Order status
"""


# from Trade_Live import Order, Trade
# from Logger_Module import my_logger, LoggerThread
from Alice_Module import *
from pya3 import *
# from Gen_Functions import *
import json
import pandas as pd
from Logger_Module import *
import os
import config


def file_writer(df, file_path):
    log_dir = 'logs' 
    if not os.path.exists(log_dir):
        os.makedirs(subject_dir)
        
    if os.path.exists(file_path):
        # Append DataFrame to the existing file
        df.to_csv(file_path, mode='a', header=False, index=False)

    else:
        # Create a new file and write the DataFrame
        df.to_csv(file_path, index=False)

# getting session id & alice object
# session_id_generate()
# if alice object remains None
if config.alice is None:
    session_id_generate()
    alice = config.alice
    logger.info(f"Session Id generated and alice var initialised: {alice}")


get_netwise_positions = alice.get_netwise_positions()
get_netwise_positions

get_holding_positions = alice.get_holding_positions()
get_holding_positions

get_daywise_positions = alice.get_daywise_positions()
get_daywise_positions

get_order_history = alice.get_order_history('')
get_order_history

get_balance =alice.get_balance()
get_balance

get_profile=alice.get_profile()
get_profile

get_trade_book = alice.get_trade_book()
get_trade_book

response = [get_netwise_positions, get_holding_positions, get_daywise_positions, get_order_history, get_balance, get_profile, get_trade_book]
name = ['get_netwise_positions', 'get_holding_positions', 'get_daywise_positions', 'get_order_history', 'get_balance', 'get_profile', 'get_trade_book']

# write to json file
i=0
for res in response:
    write_name = f"logs/{name[i]}.json"
    write_var = res

    with open(write_name, "w") as f:
        json.dump(write_var, f, indent=4)
    
    i += 1


#reading the file netposition
file = "logs/get_netwise_positions.json"
#with open(file, "r") as f:
    # positions=json.load(f)

#print(json.dumps(positions, indent =4)) 

def check_hedge() :
    """Func to check buy posn & set buy_hedge to True"""
    try: 
        global alice, ce_var, pe_var
        positions = alice.get_netwise_positions()
        logging.debug(positions)
        log_is_list = False
        if isinstance(positions, list):
            logging.info(f'positions log is a list. Continue process.')
            log_is_list = True
        else:
            logging.warning(f"Positions log is not a list: {all_trade_logs}" )
        
        if log_is_list:
            position_log_list = []
            
            for log in positions:
                qty = int(log['Netqty']) 
                print(qty)
                buy_avg_price = float(log['Buyavgprc' ])
                option_type =  log['Opttype']
                 
                position_log = {
                "Option_type": option_type, 
                "AvgPrice": buy_avg_price, 
                "Qty": qty 
                }
                position_log_list.append(position_log)
            logging.debug('all position logs appended to trade_log') 
            #print(json.dumps(trade_log_list, indent=4))
            for posn in position_log_list:
                if posn['Qty'] > 0:
                    if posn['AvgPrice'] > 0 and posn['AvgPrice']<5:
                        if posn['Option_type'] == 'CE':
                            print('CE buy hedge is True') 
                            ce_var.buy_hedge = True
                            write_obj() 
                        else:
                            print('PE buy hedge is True') 
                            pe_var.buy_hedge = True
                            write_obj() 
            
    except Exception as e:
        text = f"Error: {e}"
        logging.error(text)
        
check_hedge() 
    

    
    
          