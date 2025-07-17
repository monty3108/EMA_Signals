# from operator import index
import sys

import pandas as pd

from Alice_Module import *
import datetime

# Import the backtrader platform
import backtrader as bt
import backtrader.feeds as btfeeds

# Import for the Trade Analyzer
import csv
import os

from My_Logger import setup_logger, LogLevel

logger = setup_logger(logger_name="Back_Test_EMA200_50", log_level=LogLevel.INFO, log_to_console=config.print_logger)

# *****************Variables****************
download = True # to download scrip data from AB
plot_trades = False # to plot trades
write_result = True #to write result to the Output csv file

# Available index csv...
nifty50_csv = dict(index_csv="data/index_list/nifty50.csv", index_name='nifty50')
nifty200_csv = dict(index_csv="data/index_list/MW-NIFTY-200-25-May-2025.csv", index_name='nifty200')
midcap100_csv = dict(index_csv="data/index_list/MW-NIFTY-MIDCAP-100-25-May-2025.csv", index_name='midcap100')
smallcap100_csv = dict(index_csv="data/index_list/MW-NIFTY-SMALLCAP-100-25-May-2025.csv", index_name='smallcap100')

file_source_list = [nifty50_csv, nifty200_csv, midcap100_csv, smallcap100_csv]

# DATA_SOURCE_FILE = "data/index_list/nifty50.csv"  # mention the file name to retrieve symbols list

SYMBOL = None  # Use this symbol to carry out back testing on a single symbol otherwise make it None
CASH = 10000.0
# *****************Variables****************
RESULT_FILE_PATH = ""
DOWNLOAD_DATA_PATH = ""

def list_to_csv(output_csv, list):
    with open(output_csv, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(list)

def write_list_to_file(data_list:list, filename):
    """Writes a list to a text file, with each element on a new line.

    Args:
        data_list: The list to write.
        filename: The name of the file to write to.
    """
    with open(filename, 'w') as file:
        for item in data_list:
            file.write(str(item) + '\n')

def bt_data_list(file_path, symbol_column_name):
    """Extract symbols from a column from a csv file and return a list of symbols
    Args:
        file_path: csv file path from which list to be extracted.
        symbol_column_name: The name of the column whose elements to be added to the list.
    """
    data = pd.read_csv(file_path)
    return data[symbol_column_name].to_list()

# Trade Log

class DetailedTradeAnalyzer(bt.Analyzer):
    def __init__(self):
        self.trade_count = 0
        self.positive_trades = 0
        self.negative_trades = 0
        self.trades = []
        self.current_trade = {}
        self.cumulative_pnl = 0.0

    def notify_order(self, order):
        if order.status == order.Completed:
            if order.isbuy():
                self.current_trade = {
                    'entry_date': bt.num2date(order.executed.dt).strftime('%Y-%m-%d %H:%M:%S'),
                    'entry_price': order.executed.price,
                    'size': order.executed.size,
                    'direction': 'long'
                }
            elif order.issell() and self.current_trade:
                self.current_trade['exit_date'] = bt.num2date(order.executed.dt).strftime('%Y-%m-%d %H:%M:%S')
                self.current_trade['exit_price'] = order.executed.price

    def notify_trade(self, trade):
        if trade.isclosed:
            pnl = trade.pnl
            pnl_percent = (pnl / abs(trade.price * trade.size)) * 100 if trade.size != 0 else 0
            self.cumulative_pnl += pnl
            self.trade_count +=1
            if pnl > 0:
                self.positive_trades += 1
            else:
                self.negative_trades +=1
            self.current_trade.update({
                'pnl': round(pnl, 2),
                'pnl_percent': round(pnl_percent, 2),
                'cumulative_pnl': round(self.cumulative_pnl, 2),
                'available_cash': round(self.strategy.broker.getcash(), 2)
            })

            self.trades.append(self.current_trade.copy())
            self.current_trade.clear()

    def stop(self):
        filepath = RESULT_FILE_PATH
        with open(filepath, mode='w', newline='') as csvfile:
            fieldnames = [
                'direction','entry_date', 'size','entry_price', 'exit_date', 'exit_price',
                'pnl', 'pnl_percent', 'cumulative_pnl', 'available_cash'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for trade in self.trades:
                writer.writerow(trade)

        logger.info(f'Trade log written to: {os.path.abspath(filepath)}')

    def trade_count(self):
        return self.trade_count



# Custom Sizer: Buys max quantity based on available cash
class MaxCashSizer(bt.Sizer):
    def __init__(self, buffer=0.95):
        self.buffer = buffer  # Prevent full cash use

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy:
            size = int((cash * self.buffer) / data.close[0])
            return size
        else:
            return self.broker.getposition(data).size

# Create a Stratey
class MACD_Strategy(bt.Strategy):
    """MACD strategy
        (a) Buy when MACD buy crossover below the zero line and close above the EMA
        (b) Sell when MACD sell crossover
        params:
        period for EMA
    """
    params = (
        # Standard MACD Parameters
        ('macd1', 12),
        ('macd2', 26),
        ('macdsig', 9),
        ('atrperiod', 14),  # ATR Period (standard)
        ('atrdist', 3.0),  # ATR distance for stop price
        ('smaperiod', 30),  # SMA Period (pretty standard)
        ('dirperiod', 10),  # Lookback period to consider SMA trend direction
        ('printlog', True),
        ('period', 200),
    )

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def log(self, txt, dt=None, doprint=True):
        ''' Logging function for this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.dataclose = self.datas[0].close
        self.macd = bt.indicators.MACD(self.data,
                                       period_me1=self.p.macd1,
                                       period_me2=self.p.macd2,
                                       period_signal=self.p.macdsig)

        # Cross of macd.macd and macd.signal
        self.mcross = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)
        self.ema = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.p.period)

        # # To set the stop price
        # self.atr = bt.indicators.ATR(self.data, period=self.p.atrperiod)
        #
        # # Control market trend
        # self.sma = bt.indicators.SMA(self.data, period=self.p.smaperiod)
        # self.smadir = self.sma - self.sma(-self.p.dirperiod)


    def next(self):
        # self.log(f'{self.dataclose[0]} {self.mcross[0]} || {self.ema[0]}')

        if self.order:
            return  # pending order execution

        if not self.position:  # not in the market

            if self.mcross[0] > 0.0 and self.ema[0]<self.dataclose[0]:
                if self.macd.macd[0] < 0 and self.macd.signal[0]<0:
                    self.order = self.buy()

        else:  # in the market
            if self.mcross[0] < 0.0:
                self.close()

class EMA_Strategy(bt.Strategy):
    """EMA crossover strategy
        (a) Buy when small EMA cross up over big EMA
        (b) Sell when small EMA cross down over big EMA
        params:
        period1 for Big EMA
        period2 for Small EMA
    """
    params = (
        ('macd1', 12),
        ('macd2', 26),
        ('macdsig', 9),
        ('atrperiod', 14),  # ATR Period (standard)
        ('atrdist', 3.0),  # ATR distance for stop price
        ('smaperiod', 30),  # SMA Period (pretty standard)
        ('dirperiod', 10),  # Lookback period to consider SMA trend direction
        ('printlog', True),
        ('period1', 200),
        ('period2', 25),
    )

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                # self.log(f'{self.dataclose[0]} || {self.ema_big[0]:.2f} || {self.ema_small[0]:.2f} ||  {self.mcross[0]:.2f}')
                # self.log('BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %(order.executed.price,order.executed.value,order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                # self.log(f'{self.dataclose[0]} || {self.ema_big[0]:.2f} || {self.ema_small[0]:.2f} ||  {self.mcross[0]:.2f}')
                # self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %(order.executed.price,
                #           order.executed.value,
                #           order.executed.comm))
                pass
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        # self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
        #          (trade.pnl, trade.pnlcomm))

    def log(self, txt, dt=None, doprint=True):
        ''' Logging function for this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.dataclose = self.datas[0].close
        self.ema_big = bt.indicators.ExponentialMovingAverage(self.data,
                                       period=self.p.period1)
        self.ema_small = bt.indicators.ExponentialMovingAverage(self.data,
                                                             period=self.p.period2)

        # Cross of macd.macd and macd.signal
        self.mcross = bt.indicators.CrossOver(self.ema_small, self.ema_big)
        # self.ema = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.p.period)

        # # To set the stop price
        # self.atr = bt.indicators.ATR(self.data, period=self.p.atrperiod)
        #
        # # Control market trend
        # self.sma = bt.indicators.SMA(self.data, period=self.p.smaperiod)
        # self.smadir = self.sma - self.sma(-self.p.dirperiod)


    def next(self):
        # self.log(f'{self.dataclose[0]} || {self.ema_big[0]:.2f} || {self.ema_small[0]:.2f} ||  {self.mcross[0]:.2f}')

        if self.order:
            return  # pending order execution

        if not self.position:  # not in the market

            if self.mcross[0] > 0.0:
                    self.order = self.buy()

        else:  # in the market
            if self.mcross[0] < 0.0: # or self.dataclose[0] < self.ema_small[0]:
                self.close()

# Create a Stratey
class New_Strategy(bt.Strategy):
    """MACD strategy
        (a) Buy when MACD buy crossover below the zero line and close above the EMA
        (b) Sell when MACD sell crossover
        params:
        period for EMA
    """
    params = (
        # Standard MACD Parameters
        ('macd1', 12),
        ('macd2', 26),
        ('macdsig', 9),
        ('atrperiod', 14),  # ATR Period (standard)
        ('atrdist', 3.0),  # ATR distance for stop price
        ('smaperiod', 30),  # SMA Period (pretty standard)
        ('dirperiod', 10),  # Lookback period to consider SMA trend direction
        ('printlog', True),
        ('period', 200),
    )

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def log(self, txt, dt=None, doprint=True):
        ''' Logging function for this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.dataclose = self.datas[0].close
        self.macd = bt.indicators.MACD(self.data,
                                       period_me1=self.p.macd1,
                                       period_me2=self.p.macd2,
                                       period_signal=self.p.macdsig)

        # Cross of macd.macd and macd.signal
        self.mcross = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)
        self.ema = bt.indicators.ExponentialMovingAverage(self.data.close, period=self.p.period)

        # # To set the stop price
        # self.atr = bt.indicators.ATR(self.data, period=self.p.atrperiod)
        #
        # # Control market trend
        # self.sma = bt.indicators.SMA(self.data, period=self.p.smaperiod)
        # self.smadir = self.sma - self.sma(-self.p.dirperiod)


    def next(self):
        # self.log(f'{self.dataclose[0]} {self.mcross[0]} || {self.ema[0]}')

        if self.order:
            return  # pending order execution

        if not self.position:  # not in the market

            if self.mcross[0] > 0.0 and self.ema[0]<self.dataclose[0]:
                if self.macd.macd[0] < 0 and self.macd.signal[0]<0:
                    self.order = self.buy()

        else:  # in the market
            if self.mcross[0] < 0.0:
                self.close()

# *****************Variables****************
STRATEGY_TESTING = EMA_Strategy
# *****************Variables****************

if __name__ == '__main__':
    result = []
    if SYMBOL:
        inst_symbol = dict(symbol=SYMBOL, index='manual')
        list_of_symbols = [inst_symbol]

        # 2 yrs back
        from_date = datetime.datetime.now().replace(hour=9, minute=14, second=0) - datetime.timedelta(days=3000)
        to_date = datetime.datetime.now().replace(hour=15, minute=30, second=0) - datetime.timedelta(days=0)
        logger.info(f'Downloading scrip date range: {from_date} to {to_date}')

    else:
        list_of_symbols = []
        for source_file in file_source_list:
            print(f'Source file for back test: \n {source_file}')
            logger.info(f'Source file for back test: \n {source_file}')
            list = bt_data_list(file_path=source_file['index_csv'], symbol_column_name='SYMBOL')
            logger.debug(f'list from {source_file}:\n {list}')
            for i in list:
                list_of_symbols.append(dict(symbol=i, index=source_file['index_name']))
        logger.debug(f'Final list {list_of_symbols}\n List length: {len(list_of_symbols)}')
        # sys.exit('exit...')
        # from Gen_Functions import read_pkl
        # list_of_symbols = read_pkl("list1.pkl")

        # 2 yrs back
        from_date = datetime.datetime.now().replace(hour=9, minute=14, second=0) - datetime.timedelta(days=3000)
        to_date = datetime.datetime.now().replace(hour=15, minute=30, second=0) - datetime.timedelta(days=0)
        logger.info(f'Downloading scrip date range: {from_date} to {to_date}')

    for symbol in list_of_symbols:
        logger.debug('Entered in for loop for downloading symbols history')
        # downloading data
        if download:
            # Generating Session ID
            try:
                if config.alice is None:
                    logger.info("alice object is None. Calling get_session_id()")
                    get_session_id()
                    # session_id_generate()
                    logger.debug(f'alice obj after calling:{config.alice} ')

                # Setting alice value from config file alice obj
                alice = config.alice
            except Exception as e:
                logger.exception(e)

            # Downloading scrip data for 2 years
            try:


                inst = config.alice.get_instrument_by_symbol(exchange='NSE', symbol=symbol['symbol'])
                logger.debug(f'Inst retrieved from symbol: {inst}')
                data = config.alice.get_historical(instrument=inst, from_datetime=from_date, to_datetime=to_date,
                                                   interval="D", indices=False)
                logger.debug(f'History Data downloaded for inst {inst.symbol}')
                DOWNLOAD_DATA_PATH = f"data/{inst.symbol}.csv"
                data.to_csv(DOWNLOAD_DATA_PATH, index=False)
                RESULT_FILE_PATH = f"data/{inst.symbol}_r.csv"
            except Exception as e:
                logger.exception(e)

        else:
            # Generating Session ID
            try:
                if config.alice is None:
                    logger.info("alice object is None. Calling get_session_id()")
                    get_session_id()
                    # session_id_generate()
                    logger.debug(f'alice obj after calling:{config.alice} ')

                # Setting alice value from config file alice obj
                alice = config.alice
            except Exception as e:
                logger.exception(e)

            from_date = datetime.datetime.now().replace(hour=9, minute=14, second=0) - datetime.timedelta(days=3000)
            to_date = datetime.datetime.now().replace(hour=15, minute=30, second=0) - datetime.timedelta(days=0)
            # logger.info(f'Downloading scrip data range: {from_date} to {to_date}')

            inst = config.alice.get_instrument_by_symbol(exchange='NSE', symbol=symbol['symbol'])
            logger.debug(f'Inst retrieved from symbol: {inst}')
            # data = config.alice.get_historical(instrument=inst, from_datetime=from_date, to_datetime=to_date,
            #                                    interval="D", indices=False)
            DOWNLOAD_DATA_PATH = f"data/{inst.symbol['symbol']}.csv"
            # data.to_csv(DOWNLOAD_DATA_PATH, index=False)
            RESULT_FILE_PATH = f"data/{inst.symbol['symbol']}_r.csv"

        # Create a cerebro entity
        cerebro = bt.Cerebro()

        # Add sizer to maximize buying power (with buffer)
        cerebro.addsizer(MaxCashSizer, buffer=0.95)

        # Add a strategy
        cerebro.addstrategy(STRATEGY_TESTING)

        # Add an analyzer
        cerebro.addanalyzer(DetailedTradeAnalyzer, _name='detailed_logger')
        # Datas are in a subfolder of the samples. Need to find where the script is
        # because it could have been called from anywhere
        # modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
        # datapath = os.path.join(modpath, '../../data/sbin.csv')

        # Create a Data Feed

        # Adding feed data
        data = btfeeds.GenericCSVData(
            dataname= DOWNLOAD_DATA_PATH,

            # fromdate=datetime.datetime(2023, 5, 22),
            # todate=datetime.datetime(2025, 5, 16),

            fromdate = from_date,
            todate = to_date,
            nullvalue=0.0,

            dtformat=('%Y-%m-%d %H:%M:%S'),  # default

            datetime=0,
            open=1,
            high=2,
            low=3,
            close=4,
            volume=5,
            openinterest=-1,
            timeframe=bt.TimeFrame.Days,
            compression=1,
        )

        # Add the Data Feed to Cerebro
        cerebro.adddata(data)

        # Set our desired cash start
        cerebro.broker.setcash(CASH)

        # Print out the starting conditions
        # print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

        # Run over everything
        try:
            cerebro.run()
            final_val = f'{inst.symbol}: Final Portfolio Value: %.2f' % cerebro.broker.getvalue()
            # final_val_dict = dict(inst = inst.symbol, Final_Val=round(cerebro.broker.getvalue(),2), Trades=bt.Analyzer.trade_count)
            final_val_dict = dict(inst=inst.symbol, index=symbol['index'], Final_Val=round(cerebro.broker.getvalue(), 2))
            result.append(final_val_dict)
            print(final_val)
            # Print out the final result
            # print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
            if plot_trades:
                cerebro.plot()
        except Exception as e:
            logger.exception(e)


    # Writing results to csv and txt file
    if write_result:
        if SYMBOL:
            OUTPUT_CSV_FILE_NAME = f"data/index_list/{SYMBOL}_result.csv"
        else:
            OUTPUT_CSV_FILE_NAME = "data/index_list/EMA200_50_result.csv"
        df_final_val = pd.DataFrame(result)
        df_final_val.to_csv(OUTPUT_CSV_FILE_NAME)
        print(f"Result written in {OUTPUT_CSV_FILE_NAME}")
        write_list_to_file(data_list=result, filename='result1.txt')