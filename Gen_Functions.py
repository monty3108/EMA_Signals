# Module for General Functions which are independent 
# Ver 1.1 18 Oct 24

import datetime
import pandas as pd
import pickle
import os
from Logger_Module import my_logger
import sys
import config
import pytz
from tabulate import tabulate
from My_Logger import setup_logger, LogLevel
logger = setup_logger(logger_name="Gen Func", log_level=LogLevel.INFO, log_to_console=config.print_logger)

def next_5_min(current_min):
    """Func to calc next minute in multiple of 5"""
    list_5mins = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
    try:
        # print(current_min)
        for mins in list_5mins:
            # print(mins)
            if current_min >= 55:
                # print("here")
                next_min = 0
                break
            elif current_min >= mins:
                # print("here1")
                next_min = mins + 5
                # break
        return next_min
    except Exception as e:
        text = f"Error: {e}"
        #log(text=text, fn=fn, bot=True)
        logger.error(text)


# Function to calculate remaining seconds for next 5 mins time
def calc_next_5min():
    """Func to calc seconds remaining for next minute in multiple of 5"""
    now = datetime.datetime.now()
    next_hour = now.hour + 1
    next_min = next_5_min(current_min=now.minute)
    # next_min = next_5_min(current_min = 55)
    if next_min == 0:
        next_run_at = now.replace(hour=next_hour,
                                  minute=next_min,
                                  second=0,
                                  microsecond=0)
    else:
        next_run_at = now.replace(minute=next_min, second=0, microsecond=0)
    # print(f"Next run at : {next_run_at}")
    time_diff = next_run_at - datetime.datetime.now()
    seconds_diff = time_diff.total_seconds()
    print(seconds_diff)
    return seconds_diff


def is_holiday(today):
    """Function to return True if date passes is Holiday as per Holiday csv""" 
   
    holidays = pd.read_csv("NSE_holidays_2022.csv")
    dates_list = [
        datetime.datetime.strptime(holiday, '%d-%b-%y').date()
        for holiday in holidays['date']
     ]
    if today in dates_list:
        return True
    else:
        return False


def weekly_expiry_calculator():
    """Function to return Weekly Thursday Expiry date. """
    
    day = datetime.date.today()
    expiry = day + datetime.timedelta(days=(10 - day.weekday()) % 7)

    while is_holiday(expiry):
        expiry = expiry - datetime.timedelta(days=1)
    print(f"Weekly Expiry: {expiry}")
    return expiry


# spot & strike calc
def strike_calc(ltp, base, strike_difference=0):
    strike = round(ltp / base) * base + (strike_difference * base)
    return strike


def round_nearest(n, r=0.05):
    """Func to return number nearest to tick 0.05"""
    return round(round(n / r) * r, 2)


# Function to reverse a list
def reverse_list(lst):
    new_lst = lst[::-1]
    return new_lst


def remove_1530h(df):
    """ remove 1530 ohlc row from downloaded data"""
    drop_time = datetime.datetime.strptime("15:30:00", "%H:%M:%S").time()
    droprow = []
    for item in df.index:
        if item.time() == drop_time:
            droprow.append(item)

    df = df.drop(droprow, inplace=True)
    print(f"{droprow} row dropped")


def resample_feed(period, data):
    """Function to resample/ convert data to desired period"""
    fn = "resample"
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


#def get_var_name1(var):
#    try:
#        for name, value in globals().items():
#            if value is var:
#                print(f'Var name: {name}') 
#                return name
#    except Exception as e:
#        text = f"Error: {e}"
#        my_logger(data_to_log=text, bot=True)
#        logger.error(text)


def read_pkl(file_path):
    fn = 'read_pkl'
    try:
        with open(file_path, 'rb') as file:
            obj = pickle.load(file)

        txt = f'{file_path} reading successfully'
        logger.debug(txt)
        return obj
    except Exception as e:
        text = f"Error: {e}"
        logger.exception(text)


def write_pkl(obj, file_path):
    fn = 'write_pkl'
    try:
        with open(file_path, 'wb') as file:
            pickle.dump(obj, file)
        logger.debug(f'{file_path} written successfully.')
    except Exception as e:
        text = f"Error: {e}"
        logger.exception(text)


def create_dir(dir_name: list):
    """ create dir
    args:
    dir_name is to be passed as a list of directory """
    for directory in dir_name:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f'{directory} created successfully')


def today_date():
    """func to return today's date"""
    return datetime.date.today()

def is_holiday_today():
    """Func to exit the code if today is Holiday"""
    fn = "is_holiday_today"
    today_date = datetime.date.today()
    if is_holiday(today_date):
        msg = 'Today is holiday. Exiting the Algo.'
        my_logger(data_to_log=msg, fn=fn, bot=True)
        logger.info(msg)
        sys.exit()
    else:
        logger.info('Today is not holiday.')
        
        
def file_exist(file_path):
    """func to check existence of file""" 
    if os.path.exists(file_path):
        return True
    else:
        return False

    
def day_of_week():
    """Return current day: Mon, Tue, etc"""

    # Get today's date
    today_date = datetime.date.today()
    
    # Get the day of the week
    # Mon, Tue
    day_of_week = today_date.strftime("%a")
    
    # logger.info(f"Today's day is: {day_of_week}" )
    return day_of_week
  
 
def quit_on_days(days_list: list) :
   fn="quit_on_days" 
   """quit on given days"""
   logger.info(f"List of Days to quit program: {days_list}.")
   if day_of_week() in days_list:
       msg = f'Program is designed to quit on {day_of_week()}. Exiting......... '
       print(msg) 
       my_logger(data_to_log=msg, fn=fn, bot=True) 
       logger.info(msg)
       sys.exit(1)

def get_time():
    current_time = datetime.datetime.now(pytz.timezone('ASIA/KOLKATA')).time()
    return current_time

def csv_column_to_list(file_path, symbol_column_name):
    """Extract symbols from a column from a csv file and return a list of symbols
    Args:
        file_path: csv file path from which list to be extracted.
        symbol_column_name: The name of the column whose elements to be added to the list.
    """
    data = pd.read_csv(file_path)
    return data[symbol_column_name].to_list()


def df_to_text(file_path, df):
    """
    Convert pandas df to tabular text file

    Args:
        file_path (str): The path to the text file.
        df: source df to convert into text file.
    """
    headers = df.columns.tolist()
    table_data = df.values.tolist()

    # Define output file names for different styles
    output_file_tabulate_grid = file_path
    # output_file_tabulate_fancy_grid = 'dataframe_output_tabulate_fancy_grid.txt'
    # output_file_tabulate_pipe = 'dataframe_output_tabulate_pipe.txt'

    # Write using 'grid' format
    with open(output_file_tabulate_grid, 'w') as f:
        f.write(tabulate(table_data, headers=headers, tablefmt="grid"))
    print(f"DataFrame written to '{output_file_tabulate_grid}' using tabulate (grid).")

def clear_console():
    """
    Clears the console screen based on the operating system.
    """
    # For Windows
    if os.name == 'nt':
        _ = os.system('cls')
    # For macOS and Linux (posix refers to POSIX compliant systems)
    else:
        _ = os.system('clear')