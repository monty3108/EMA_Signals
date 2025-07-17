from operator import index

from Alice_Module import *
import datetime

# Import the backtrader platform
import backtrader as bt
import backtrader.feeds as btfeeds


from My_Logger import setup_logger, LogLevel

logger = setup_logger(logger_name="Nifty Buy", log_level=LogLevel.INFO, log_to_console=config.print_logger)

download = True

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
        # 2 yrs back
        from_date = datetime.datetime.now().replace(hour=9, minute=14, second=0) - datetime.timedelta(days=2825)
        to_date = datetime.datetime.now().replace(hour=15, minute=30, second=0) - datetime.timedelta(days=0)
        logger.info(f'Downloading scrip data range: {from_date} to {to_date}')

        inst = config.alice.get_instrument_by_symbol(exchange='NSE', symbol="SBIN")
        print(inst)
        data = config.alice.get_historical(instrument=inst, from_datetime=from_date, to_datetime=to_date,
                                               interval="D", indices=False)
        data.to_csv("data/tcs.csv", index=False)

    except Exception as e:
        logger.exception(e)


# Create a Stratey
class MACD_Strategy(bt.Strategy):
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


if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(MACD_Strategy)

    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
    # modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    # datapath = os.path.join(modpath, '../../data/sbin.csv')

    # Create a Data Feed
    data = btfeeds.GenericCSVData(
        dataname='data/tcs.csv',

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
    cerebro.broker.setcash(100000.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # Print out the final result
    # print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    # cerebro.plot()