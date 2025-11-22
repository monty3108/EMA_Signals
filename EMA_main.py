"""
Created on: Tuesday, June 10, 2025
@author: Monty
Description: EMA 50/200 Crossover Strategy for Live Trading Signal Generation.
"""
import sys

import pandas as pd
import pandas_ta as ta
from Alice_Module import *
from Gen_Functions import get_time, csv_column_to_list, create_dir, write_pkl, read_pkl, df_to_text, clear_console, remove_files
import datetime as dt
import config
import time
import file_operate
from dateutil.relativedelta import relativedelta
import dividend_record

from My_Logger import setup_logger, LogLevel

logger = setup_logger(logger_name="EMA_200_50", log_level=LogLevel.INFO, log_to_console=config.print_logger)
date_str = datetime.datetime.now().strftime("%d_%m_%Y")

# for notification on Telegram
from Notification_Module import notify, stop_worker, notify1, send_docs

# create required directories
create_dir(config.dir_name)
strategy_name = 'EMA200_50'
start_time = time.time() # for calculation of elapsed time

# Variable for alice operations
ALICE = None
# for telegram notifications
def me(msg):
    """For sending personal notification """
    st = strategy_name
    text = f'{msg} ({st})'
    notify1(text)

def group(msg):
    """For sending group notification """
    st = strategy_name
    text = f'{msg} ({st})'
    notify(text)

def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.0f} s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.0f} m"
    elif seconds < 86400: # 60 * 60 * 24
        hours = seconds / 3600
        return f"{hours:.2f} h"
    else:
        days = seconds / 86400
        return f"{days:.2f} days"

# Function to display the result
def display_age(birth_date_str, relative_date_str=None):
    def calculate_age(birth_date_str, relative_date_str=None):
        # Convert the birth date string to a datetime object
        birth_date = dt.datetime.strptime(birth_date_str, "%d %b %Y")
        # print(birth_date)

        # Get the current date
        current_date = dt.datetime.today()
        if relative_date_str:
            relative_date_str = dt.strptime(relative_date_str, "%Y-%m-%d")
            delta = relativedelta(relative_date_str, birth_date)
        else:
            # Calculate the difference using relativedelta
            delta = relativedelta(current_date, birth_date)

        # Return the age in years, months, and days
        return delta.years, delta.months, delta.days
    age = calculate_age(birth_date_str, relative_date_str)
    if age:
        years, months, days = age
        return f"{years} y, {months} m, {days} d"
    else:
        return "Error."

def symbol_list_to_inst_list(symbol_list: list):
    inst_list = []
    try:
        for symbol in symbol_list:
            symbol['inst'] = config.alice.get_instrument_by_symbol(exchange='NSE', symbol=symbol['symbol'])
            inst_list.append(symbol)
        return inst_list
    except Exception as e:
        logger.exception(e)

def generate_live_signal(df: pd.DataFrame):
    """
    Analyzes the latest data in the DataFrame to generate a trading signal.

    Args:
        df (pd.DataFrame): DataFrame with a 'close' column and a datetime index.
                           It should contain all historical data up to the present.

    Returns:
        tuple: A tuple containing the signal string ('Buy', 'Sell', 'Hold')
               and a descriptive reason for the signal.
    """
    # --- Step A: Ensure there's enough data ---
    # We need at least 201 data points to get a reliable EMA_200 and check for a crossover.
    if len(df) < 201:
        return "Hold", "Not enough data for 200-period EMA calculation."

    # --- Step B: Calculate the latest EMAs ---
    # We calculate the EMAs for the entire series to ensure they are accurate.
    # pandas_ta is efficient and will handle this quickly.
    df.ta.ema(length=50, append=True, col_name="EMA_50")
    df.ta.ema(length=200, append=True, col_name="EMA_200")

    # --- Step C: Get the last two data points ---
    # We need the most recent ([-1]) and the second most recent ([-2]) rows
    # to check if a crossover just happened.
    latest = df.iloc[-1]
    previous = df.iloc[-2]

    # --- Step D: Implement the Crossover Logic ---
    # Check for a Golden Cross (Buy Signal)
    # Condition 1: The fast EMA was below the slow EMA in the *previous* period.
    # Condition 2: The fast EMA is now above the slow EMA in the *latest* period.
    if previous['EMA_50'] < previous['EMA_200'] and latest['EMA_50'] > latest['EMA_200']:
        signal = "Buy"
        reason = f"Golden Cross: EMA_50 ({latest['EMA_50']:.2f}) crossed above EMA_200 ({latest['EMA_200']:.2f})."
        return signal, reason

    # Check for a Death Cross (Sell Signal)
    # Condition 1: The fast EMA was above the slow EMA in the *previous* period.
    # Condition 2: The fast EMA is now below the slow EMA in the *latest* period.
    if previous['EMA_50'] > previous['EMA_200'] and latest['EMA_50'] < latest['EMA_200']:
        signal = "Sell"
        reason = f"Death Cross: EMA_50 ({latest['EMA_50']:.2f}) crossed below EMA_200 ({latest['EMA_200']:.2f})."
        return signal, reason

    # If neither cross occurred, the signal is to hold.
    signal = "Hold"
    reason = f"No Crossover: EMA_50 ({latest['EMA_50']:.2f}), EMA_200 ({latest['EMA_200']:.2f})."
    return signal, reason

def get_ltp(instrument):
    fn = 'scrip_info'
    # to get scrip info by providing proper scrip name
    try:
        # print(instrument)
        info = config.alice.get_scrip_info(instrument)
        if info['stat'] == 'Not_Ok':
            print(f"scrip info: {info}")
            sys.exit('exiting program....')
        # high = info['High']
        # low = info['Low']
        # open = info['openPrice']
        # prev_close = info['PrvClose']
        ltp = info['LTP']
        # print(f"Open: {open}\n"
        #       f"High: {high}\n"
        #       f"Low : {low}\n"
        #       f"Prev close: {prev_close}\n"
        #       f"ltp: {ltp}")
        return float(ltp)
    except Exception as e:
        text = f"Error: {e}."
        print(text)

def generate_reports(file_path):
        """
        Generates Holding and Sold reports from a stock transaction ledger (CSV).

        Args:
            file_path (str): The path to the CSV ledger file (e.g., 'positions.csv').
        """
        print("Starting report generation...")

        # --- 1. Load and Prepare Data ---
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"Error: File not found at {file_path}")
            return

        # Standardizing column names for easier access (optional but good practice)
        df.columns = df.columns.str.lower().str.replace('[^a-z0-9_]', '', regex=True)

        # Data type conversion
        df['date'] = pd.to_datetime(df['date'], dayfirst=True)  # Adjust dayfirst based on your date format
        df['qty'] = pd.to_numeric(df['qty'])
        df['price'] = pd.to_numeric(df['price'])

        # Initialize a column to track the remaining quantity of each BUY transaction
        df['remaining_qty'] = np.where(df['tran_type'] == 'BUY', df['qty'], 0)

        # --- 2. Process Sales (FIFO Logic) ---

        sold_transactions = []

        # Iterate through all SELL transactions
        for _, sell_row in df[df['tran_type'] == 'SELL'].iterrows():
            stock = sell_row['stock_name']
            sell_qty = sell_row['qty']
            sell_price = sell_row['price']

            # Select all *unmatched* BUY transactions for the same stock, sorted by date (FIFO)
            buy_matches = df[
                (df['stock_name'] == stock) &
                (df['tran_type'] == 'BUY') &
                (df['remaining_qty'] > 0)
                ].sort_values(by='date')

            # Match the SELL quantity against the BUY transactions in FIFO order
            qty_to_match = sell_qty

            for buy_index, buy_row in buy_matches.iterrows():
                if qty_to_match <= 0:
                    break

                # Quantity being matched from this specific BUY lot
                qty_matched_from_lot = min(qty_to_match, buy_row['remaining_qty'])

                if qty_matched_from_lot > 0:
                    # Update the remaining quantity in the main DataFrame (in place)
                    df.loc[buy_index, 'remaining_qty'] -= qty_matched_from_lot

                    # Calculate profit for this specific portion of the sale
                    buy_cost_basis = buy_row['price']
                    profit = (sell_price - buy_cost_basis) * qty_matched_from_lot

                    # Record the matched portion for the Sold Report
                    sold_transactions.append({
                        'stock_name': stock,
                        'qty_sold': qty_matched_from_lot,
                        'buy_price': buy_cost_basis,
                        'sell_price': sell_price,
                        'profit': profit
                    })

                    qty_to_match -= qty_matched_from_lot

        # --- 3. Create Sold Report ---

        if sold_transactions:
            sold_df = pd.DataFrame(sold_transactions)

            # Group by stock name to get the final summarized report
            sold_report = sold_df.groupby('stock_name').agg(
                qty_sold=('qty_sold', 'sum'),
                total_buy_value=('buy_price', lambda x: (x * sold_df.loc[x.index, 'qty_sold']).sum()),
                total_sell_value=('sell_price', lambda x: (x * sold_df.loc[x.index, 'qty_sold']).sum()),
                profit=('profit', 'sum')
            ).reset_index()

            # Calculate the final average buy and sell prices
            sold_report['avg_buy_price'] = sold_report['total_buy_value'] / sold_report['qty_sold']
            sold_report['avg_sell_price'] = sold_report['total_sell_value'] / sold_report['qty_sold']

            # Select and format final columns
            sold_report_final = sold_report[[
                'stock_name', 'avg_buy_price', 'avg_sell_price', 'qty_sold', 'profit'
            ]]

            # Save the Sold Report
            sold_report_final.to_csv('sold_report.csv', index=False, float_format='%.2f')
            print("✅ Sold Report saved to 'sold_report.csv'")
        else:
            print("ℹ️ No SELL transactions found or processed.")

        # --- 4. Create Holding Report ---

        # Filter for remaining BUY lots (where remaining_qty > 0)
        holding_lots = df[
            (df['tran_type'] == 'BUY') &
            (df['remaining_qty'] > 0)
            ].copy()

        # Calculate the total value of the remaining holding for weighted average
        holding_lots['total_cost'] = holding_lots['remaining_qty'] * holding_lots['price']

        # Group by stock name
        holding_report = holding_lots.groupby('stock_name').agg(
            qty_holding=('remaining_qty', 'sum'),
            total_cost=('total_cost', 'sum')
        ).reset_index()

        # Calculate the average buy price (Total Cost / Total Qty)
        holding_report['avg_buy_price'] = holding_report['total_cost'] / holding_report['qty_holding']

        # Select and format final columns
        holding_report_final = holding_report[[
            'stock_name', 'qty_holding', 'avg_buy_price'
        ]]

        # Save the Holding Report
        holding_report_final.to_csv('holding_report.csv', index=False, float_format='%.2f')
        print("✅ Holding Report saved to 'holding_report.csv'")

def view_position_status():
    # file_path = 'consolidated.csv'
    generate_reports('positions.csv')
    file_path = 'holding_report.csv'
    df = pd.read_csv(file_path)



    df['qty_holding'] = pd.to_numeric(df['qty_holding'])
    df['avg_buy_price'] = pd.to_numeric(df['avg_buy_price'])

    df['invested'] = df['qty_holding'] * df['avg_buy_price']
    df['holding'] = None
    df['ltp'] = None
    df['profit']=None
    df['percent']= None
    df['remarks']= None
    total_invested = df['invested']. sum() 

    # print(df)
    total_profit = 0
    for i in range( len(df)):
        row = df.iloc[i]
        inst = config.alice.get_instrument_by_symbol(exchange='NSE', symbol=row['stock_name'])
        ltp = get_ltp(instrument=inst)

        # sys.exit()
        
        current_value = (row['qty_holding'] * ltp)
        invested = row['invested']
        profit = round(current_value - invested, 2)
        percent = round((profit/invested) * 100, 2)
        if percent > 10:
            remarks = f'Book profit'
        elif percent < -10:
            remarks = f'Buy more'
        else:
            remarks = ''
        df.loc[i,'holding'] = f'{df.loc[i,'qty_holding']} x {df.loc[i,'avg_buy_price']}'
        df.loc[i, 'ltp'] = f'{ltp}'
        df.loc[i, 'profit'] = f'{profit} ({percent}%)'
        df.loc[i, 'percent'] = percent
        df.loc[i, 'remarks'] = remarks
        
        total_profit += profit
        
    current_value= round(total_invested + total_profit,2)
    position_summary = dict(Invested=round(total_invested,2), 
    Current_value=current_value, PnL=round(total_profit,2)) 
    msg1 = json.dumps(position_summary, indent =4)
    group(msg1) 
    # print(df)

    df.rename(columns={'stock_name': 'stock'}, inplace=True)
    # copy the df to df_pnl
    df_pnl = df.copy()

    df = df.drop(['qty_holding','avg_buy_price', 'percent'], axis=1)
    # df.rename(columns={'stock_name': 'stock'}, inplace=True)
    df['index'] = range(1, len(df) + 1) # Final df to send

    # format df_pnl for sending
    df_pnl.sort_values(by=['percent', 'stock'],
                       ascending=[True, True], inplace=True)
    df_pnl['index'] = range(1, len(df_pnl) + 1)
    # pd.set_option('display.max_colwidth', None)

    # sys.exit()
    new_order = ['index', 'stock', 'profit', 'remarks', 'holding', 'ltp', 'invested']
    df_pnl_reordered = df_pnl[new_order]

    # Step 1: Get the current time
    current_time = get_time()  # datetime.now()

    # Step 2: Format the time into HH:MM string
    ts = current_time.strftime("%H_%M")

    holding_file_path = f'pkl_obj/Holding_{ts}.txt'
    df_to_text(file_path=holding_file_path, df=df)

    pnl_file_path = f'pkl_obj/PnL_{ts}.txt'
    df_to_text(file_path=pnl_file_path, df=df_pnl_reordered)


    df_sold_report = pd.read_csv('sold_report.csv')
    sold_report_path = f'pkl_obj/sold_report_{ts}.txt'
    df_to_text(file_path=sold_report_path, df=df_sold_report)


    files = [holding_file_path, pnl_file_path, sold_report_path]
    send_docs(docs=files)
    remove_files(files)
    time.sleep(2)
    return

def view_stock_transactions():
    # reading the csv file
    file_path = 'positions.csv'
    df = pd.read_csv(file_path)

    df_positions = df.copy()

    df['ltp'] = None

    # getting the ltp
    for i in range(len(df)):
        row = df.iloc[i]
        # print(row)
        inst = config.alice.get_instrument_by_symbol(exchange='NSE', symbol=row['stock_name'])
        # print(inst)
        ltp = get_ltp(instrument=inst)
        df.loc[i, 'ltp'] = f'{ltp}'
        # print(ltp)

    # dropping unnecessary columns
    # df = df.drop(['demat'], axis=1)
    df['holding age'] = df.apply(lambda row: display_age(row['date']), axis=1)
    # The 'inplace=True' argument modifies the DataFrame directly
    # 'ascending=True' sorts from A-Z (or low to high)
    # df.sort_values(by="stock_name", ascending=True, inplace=True)
    df['date'] = pd.to_datetime(df['date'], format='%d %b %Y', errors='coerce')
    df.sort_values(by=['stock_name', 'date' ], ascending=[True, True], inplace=True)
    df.rename(columns={'stock_name': 'stock', 'tran_type': 'type'}, inplace=True)
    df['date'] = df['date'].dt.strftime('%d-%b-%Y')
    df['index'] = range(1, len(df) + 1)


    new_order = ['index', 'date', 'stock', 'qty', 'price', 'ltp', 'type', 'holding age', 'demat']
    df = df[new_order]

    # Step 1: Get the current time
    current_time = get_time()  # datetime.now()

    # Step 2: Format the time into HH:MM string
    ts = current_time.strftime("%H_%M")

    # position file
    position_file_path = f'pkl_obj/Positions_{ts}.txt'
    df_to_text(file_path=position_file_path, df=df_positions)

    # transaction file
    transaction_file_path = f'pkl_obj/All_Trans_{ts}.txt'
    df_to_text(file_path=transaction_file_path, df=df)

    files = [transaction_file_path, position_file_path]
    send_docs(docs=files)
    time.sleep(2)
    print_android('Stock trans sent to the telegram.....')
    remove_files(files)
    return

    #
    # list = csv_column_to_list(file_path=file_path, symbol_column_name='stock_name')
    # inst_list = []
    # for symbol in list:
    #     inst = config.alice.get_instrument_by_symbol(exchange='NSE', symbol=symbol)
    #     inst_list.append(inst)
    # print(inst_list)
    
def view_monthly_investment():
    """Func to calculate monthly investment from positions.csv
    and send as a text file """
    text_file_path = f"Inv_Monthly_{date_str}.txt"
    # Load the dataframe.
    df_positions = pd.read_csv('positions.csv')
    
    # Calculate the invested amount for each entry.
    df_positions['invested_amount'] = df_positions['qty'] * df_positions['price']
    
    # Convert the 'date' column to datetime objects.
    df_positions['date'] = pd.to_datetime(df_positions['date'], format='%d %b %Y')
    
    # Create a new column for the month of investment.
    df_positions['investment_month'] = df_positions['date'].dt.to_period('M')
    
    # Group the entries by month and calculate the total monthly investment.
    monthly_investment = df_positions.groupby('investment_month')['invested_amount'].sum().reset_index()
    
    # Rename the columns for clarity.
    monthly_investment.columns = ['Month', 'Monthly Investment']
    
    df_to_text(file_path=text_file_path, df=monthly_investment)
    files = [text_file_path]
    send_docs(docs=files)
    print_android('Investment monthly sent to the telegram.....')
    
    remove_files([text_file_path])
    
    # Save the final data to a new CSV file.
    # monthly_investment.to_csv('monthly_investment.csv', index=False)

def print_android(str):
    space = 4 * " "
    print(f'{space}{str}')

def string_to_boolean(s):
    s = s.lower()
    if s in ('true', 't', 'yes', 'y', '1'):
        return True
    elif s in ('false', 'f', 'no', 'n', '0'):
        return False
    else:
        # Handle invalid input, e.g., raise an error or return None
        raise ValueError(f"Cannot convert '{s}' to boolean")

def get_ema_signals():
    path_list_of_inst = "pkl_obj/list_of_inst.pkl"
    path_bt_result = "pkl_obj/bt_result.pkl"
    SKIP_CURRENT_DAY = False  # set true if last day alert to calculate
    re_write_inst_csv = False
    print_android('Default boolean values')
    print_android(f'Skip Current Day: {SKIP_CURRENT_DAY}')
    print_android(f'Re-write Inst CSV to pkl: {re_write_inst_csv}')
    modify_confirm = input("Confirm modification for these values? (y/n): ").lower()
    if modify_confirm == 'y':
        print_android("Enter new values (leave blank to keep current):")

        new_skip_str = input(f"Skip Current Day:  ({SKIP_CURRENT_DAY}): ")
        if new_skip_str:
            SKIP_CURRENT_DAY = string_to_boolean(new_skip_str)

        new_re_write_str = input(f"Re-write Inst CSV to pkl:  ({re_write_inst_csv}): ")
        if new_re_write_str:
            re_write_inst_csv = string_to_boolean(new_re_write_str)

    # write inst csv
    if re_write_inst_csv:
        # Available index csv...
        nifty50_csv = dict(index_csv="data/index_list/nifty50.csv", index_name='nifty50')
        nifty200_csv = dict(index_csv="data/index_list/MW-NIFTY-200-25-May-2025.csv", index_name='nifty200')
        midcap100_csv = dict(index_csv="data/index_list/MW-NIFTY-MIDCAP-100-25-May-2025.csv", index_name='midcap100')
        smallcap100_csv = dict(index_csv="data/index_list/MW-NIFTY-SMALLCAP-100-25-May-2025.csv",
                               index_name='smallcap100')
        # nifty50_csv = "data/index_list/nifty50.csv"
        # nifty200_csv = "data/index_list/MW-NIFTY-200-25-May-2025.csv"
        # midcap100_csv = "data/index_list/MW-NIFTY-MIDCAP-100-25-May-2025.csv"
        # smallcap100_csv = "data/index_list/MW-NIFTY-SMALLCAP-100-25-May-2025.csv"
        logger.info('Adding source started')
        file_source_list = [nifty50_csv, nifty200_csv, midcap100_csv, smallcap100_csv]
        # file_source_list = [smallcap100_csv]
        # bt result csv

        # reading result csv
        bt_result = pd.read_csv('data/index_list/EMA200_50_result.csv')
        write_pkl(obj=bt_result, file_path=path_bt_result)


        list_of_symbols = []
        for source_file in file_source_list:
            # print(f'Source file for back test: \n {source_file}')
            logger.debug(f'Source file for back test: \n {source_file}')
            list = csv_column_to_list(file_path=source_file['index_csv'], symbol_column_name='SYMBOL')
            logger.debug(f'list from {source_file}:\n {list}')
            for i in list:
                list_of_symbols.append(dict(symbol=i, index=source_file['index_name']))
        logger.debug(f'Final list {list_of_symbols}\n List length: {len(list_of_symbols)}')

        list_of_inst = symbol_list_to_inst_list(symbol_list=list_of_symbols)  # returns dict keys(symbol, index, inst)
        # saving list_of_inst to pkl
        write_pkl(obj=list_of_inst, file_path=path_list_of_inst)

        # reading csv pkl files
    list_of_inst = read_pkl(file_path=path_list_of_inst)
    bt_result = read_pkl(file_path=path_bt_result)

    total_symbols = len(list_of_inst)
    logger.info('Source file added')
    symbol_count = 1
    filtered_symbols = []
    for inst in list_of_inst:
        try:
            time_now = time.time()
            elapsed_time = format_time(time_now - start_time)
            print(f'   {elapsed_time}: {symbol_count} /{total_symbols}: Checking {inst['symbol']} | {inst['index']} ')

            # if symbol_count > 2:
            #     logging.info('breaking loop')
            #     logger.debug(f'\n {df.tail(2)}')
            #     break
            df = history_resample(inst=inst['inst'], days_back=3000, resample_period='1d',
                                  skip_current_day=SKIP_CURRENT_DAY)
            logger.debug(f'Inst: {inst['inst'].symbol} last two days data returned from history_resample')
            logger.debug(f'\n {df.tail(2)}')
            if symbol_count < 2:
                print(f'{symbol_count} {inst['inst'].symbol}\n {df.tail(2)}')
                time.sleep(3)
                cont_program = input('Continue (y/n) : ')
                if not string_to_boolean(cont_program):
                    break

            symbol_count += 1

            # Get the Signal for the CURRENT DAY ---
            # You would fetch all available historical data into `my_latest_data_df`
            # and then call the function once.
            # Assume `df` is the most up-to-date data you have
            my_latest_data_df = df.copy()
            logger.debug(f'Checking for inst: {inst['symbol']} | {inst['index']}.....')
            final_signal, final_reason = generate_live_signal(my_latest_data_df)
            if not final_signal == "Hold":
                # print("=" * 50)
                # print("Generating signal for the most recent data point...")
                print("=" * 50)
                current_date = my_latest_data_df.index[-1].strftime('%Y-%m-%d')
                print(f"Inst: {inst['inst'].symbol} | {inst['index']}")
                print(f"Analysis Date: {current_date}")
                print(f"SIGNAL: {final_signal}")
                print(f"REASON: {final_reason}")
                print("=" * 50)
                bt = bt_result[bt_result['inst'] == inst['inst'].symbol]
                dict1 = dict(Analysis_Date=current_date, Inst=inst['inst'].symbol, Index=inst['index'],
                             Signal=final_signal,
                             BT=bt['Final_Val'].iloc[0])
                filtered_symbols.append(dict1)
                # print(filtered_symbols)
                # msg = f'Analysis Date: {current_date} | Inst: {inst['inst'].symbol} | SIGNAL: {final_signal}'
                # group(msg)
        except Exception as e:
            logger.exception(e)

    msg = json.dumps(filtered_symbols, indent=4)
    logger.info(msg)
    print(msg)
    group(msg)
    logger.info(elapsed_time)


def get_holding_signals():
    path_list_of_inst = "pkl_obj/position_list_of_inst.pkl"
    path_bt_result = "pkl_obj/bt_result.pkl"
    SKIP_CURRENT_DAY = False  # set true if last day alert to calculate
    re_write_inst_csv = False
    print_android('Default boolean values')
    print_android(f'Skip Current Day: {SKIP_CURRENT_DAY}')
    print_android(f'Re-write Inst CSV to pkl: {re_write_inst_csv}')
    modify_confirm = input("Confirm modification for these values? (y/n): ").lower()
    if modify_confirm == 'y':
        print_android("Enter new values (leave blank to keep current):")

        new_skip_str = input(f"Skip Current Day:  ({SKIP_CURRENT_DAY}): ")
        if new_skip_str:
            SKIP_CURRENT_DAY = string_to_boolean(new_skip_str)

        new_re_write_str = input(f"Re-write Inst CSV to pkl:  ({re_write_inst_csv}): ")
        if new_re_write_str:
            re_write_inst_csv = string_to_boolean(new_re_write_str)

    # write inst csv
    if re_write_inst_csv:
        # Available index csv...
        consolidated_csv = dict(index_csv="consolidated.csv", index_name='Holding')

        # nifty50_csv = "data/index_list/nifty50.csv"
        # nifty200_csv = "data/index_list/MW-NIFTY-200-25-May-2025.csv"
        # midcap100_csv = "data/index_list/MW-NIFTY-MIDCAP-100-25-May-2025.csv"
        # smallcap100_csv = "data/index_list/MW-NIFTY-SMALLCAP-100-25-May-2025.csv"

        logger.info('Adding source started')
        # making list
        file_source_list = [consolidated_csv]

        # reading result csv
        bt_result = pd.read_csv('data/index_list/EMA200_50_result.csv')
        write_pkl(obj=bt_result, file_path=path_bt_result)

        list_of_symbols = []
        for source_file in file_source_list:
            # print(f'Source file for back test: \n {source_file}')
            logger.debug(f'Source file for back test: \n {source_file}')
            list = csv_column_to_list(file_path=source_file['index_csv'], symbol_column_name='stock_name')
            logger.debug(f'list from {source_file}:\n {list}')
            for i in list:
                list_of_symbols.append(dict(symbol=i, index=source_file['index_name']))
        logger.debug(f'Final list {list_of_symbols}\n List length: {len(list_of_symbols)}')

        list_of_inst = symbol_list_to_inst_list(symbol_list=list_of_symbols)  # returns dict keys(symbol, index, inst)
        # saving list_of_inst to pkl
        write_pkl(obj=list_of_inst, file_path=path_list_of_inst)

        # reading csv pkl files
    list_of_inst = read_pkl(file_path=path_list_of_inst)
    bt_result = read_pkl(file_path=path_bt_result)

    total_symbols = len(list_of_inst)
    logger.info('Source file added')
    symbol_count = 1
    filtered_symbols = []
    for inst in list_of_inst:
        try:
            time_now = time.time()
            elapsed_time = format_time(time_now - start_time)
            print(f'   {elapsed_time}: {symbol_count} /{total_symbols}: Checking {inst['symbol']} | {inst['index']} ')

            # if symbol_count > 2:
            #     logging.info('breaking loop')
            #     logger.debug(f'\n {df.tail(2)}')
            #     break
            df = history_resample(inst=inst['inst'], days_back=3000, resample_period='1d',
                                  skip_current_day=SKIP_CURRENT_DAY)
            logger.debug(f'Inst: {inst['inst'].symbol} last two days data returned from history_resample')
            logger.debug(f'\n {df.tail(2)}')
            if symbol_count < 2:
                print(f'{symbol_count} {inst['inst'].symbol}\n {df.tail(2)}')
                time.sleep(3)
                cont_program = input('Continue (y/n) : ')
                if not string_to_boolean(cont_program):
                    break

            symbol_count += 1

            # Get the Signal for the CURRENT DAY ---
            # You would fetch all available historical data into `my_latest_data_df`
            # and then call the function once.
            # Assume `df` is the most up-to-date data you have
            my_latest_data_df = df.copy()
            logger.debug(f'Checking for inst: {inst['symbol']} | {inst['index']}.....')
            final_signal, final_reason = generate_live_signal(my_latest_data_df)
            if not final_signal == "Hold":
                # print("=" * 50)
                # print("Generating signal for the most recent data point...")
                print("=" * 50)
                current_date = my_latest_data_df.index[-1].strftime('%Y-%m-%d')
                print(f"Inst: {inst['inst'].symbol} | {inst['index']}")
                print(f"Analysis Date: {current_date}")
                print(f"SIGNAL: {final_signal}")
                print(f"REASON: {final_reason}")
                print("=" * 50)
                bt = bt_result[bt_result['inst'] == inst['inst'].symbol]
                dict1 = dict(Analysis_Date=current_date, Inst=inst['inst'].symbol, Index=inst['index'],
                             Signal=final_signal,
                             BT=bt['Final_Val'].iloc[0])
                filtered_symbols.append(dict1)
                # print(filtered_symbols)
                # msg = f'Analysis Date: {current_date} | Inst: {inst['inst'].symbol} | SIGNAL: {final_signal}'
                # group(msg)
        except Exception as e:
            logger.exception(e)

    msg = json.dumps(filtered_symbols, indent=4)
    logger.info(msg)
    print(msg)
    group(msg)
    logger.info(elapsed_time)

def file_operation_menu():
    while True:
        print_android("\n--- File Operation Menu ---")
        print_android("1. Operate position file")
        print_android("2. Send holding positions")
        print_android("3. Get EMA signals")
        print_android("4. Get holding signals")
        print_android("5. Operate Dividend file")
        print_android("6. Generate Session for live quotes")
        print_android("10. Clear console")  # Shifted
        print_android("0. Exit")  # New option


        choice = input("Enter your choice (1-4): ")

        if choice == '1':
            clear_console()
            file_operate.file_operate('positions.csv')

        elif choice == '2':
            # send_positions()
            view_position_status()
            print_android('Positions sent to the telegram.')
            view_stock_transactions()
            view_monthly_investment()

        elif choice == '3':
            get_ema_signals()

        elif choice == '4':
            get_holding_signals()

        elif choice == '5':
            dividend_record.main_menu()

        elif choice == '6':
            generate_session()

        elif choice == '0':  # New delete functionality

            break  # Update the DataFrame with the result of deletion

        elif choice == '10':

            print_android("Clearing Console.")
            time.sleep(1)  # Pause for 3 seconds so you can see the text before it clears
            clear_console()
        else:
            print_android("Invalid choice. Please enter a number between 1 and 4.")

def generate_session():
    global ALICE
    if config.alice is None:
        logger.debug("alice object is None. Calling get_session_id()")
        get_session_id()
        # session_id_generate()
        logger.debug(f'alice obj after calling:{config.alice} ')
    # Setting alice value from config file alice obj
    ALICE = config.alice


if __name__ == '__main__':
    # sys.exit()
    file_operation_menu()
    logger.info("Program finished....")
    stop_worker()

