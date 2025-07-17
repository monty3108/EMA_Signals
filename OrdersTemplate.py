import backtrader as bt

class MyStrategy(bt.Strategy):
    params = (
        ('stop_loss_pct', 0.02),  # Stop loss percentage
        ('target_profit_pct', 0.05),  # Target profit percentage
    )

    def __init__(self):
        self.order = None  # To keep track of the main order (buy/sell)
        self.stop_order = None  # To keep track of the stop loss order
        self.target_order = None  # To keep track of the target profit order
        self.buy_price = None  # To store the price at which we bought

    def log(self, txt, dt=None):
        """ Logging function for this strategy"""
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()}, {txt}')

    def next(self):
        if self.order:
            return  # Waiting for pending orders to be completed

        if not self.position:  # Check if we are not in the market
            self.log(f'BUY CREATE, {self.data.close[0]:.2f}')
            self.order = self.buy()  # Place a buy order

        elif self.position and self.stop_order is None and self.target_order is None:  # We have a position but no stop/target order
            # Calculate stop price
            self.buy_price = self.position.price
            stop_price = self.buy_price * (1.0 - self.params.stop_loss_pct)
            target_price = self.buy_price * (1.0 + self.params.target_profit_pct)

            self.log(f'STOP LOSS ORDER CREATED at {stop_price:.2f}')
            self.stop_order = self.sell(exectype=bt.Order.Stop, price=stop_price)  # Place stop loss order

            self.log(f'TARGET ORDER CREATED at {target_price:.2f}')
            self.target_order = self.sell(exectype=bt.Order.Limit, price=target_price)  # Place target order

        # Check if the stop order or target order is pending
        if self.stop_order and self.stop_order.status in [bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted]:
            self.log('STOP ORDER IS PENDING')
        if self.target_order and self.target_order.status in [bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted]:
            self.log('TARGET ORDER IS PENDING')

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, Price: {order.executed.price:.2f}')
            elif order.issell():
                self.log(f'SELL EXECUTED, Price: {order.executed.price:.2f}')
            
            # If either stop or target order is executed, cancel the other one
            if order == self.stop_order:
                self.log('STOP ORDER EXECUTED')
                self.cancel(self.target_order)  # Cancel target order if stop is executed
            elif order == self.target_order:
                self.log('TARGET ORDER EXECUTED')
                self.cancel(self.stop_order)  # Cancel stop order if target is executed
            
            # Reset orders to None
            self.stop_order = None
            self.target_order = None

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            self.order = None  # Reset the order variable

        # Track the stop and target orders separately
        if order == self.stop_order and order.status in [order.Completed, order.Canceled, order.Rejected]:
            self.stop_order = None  # Reset the stop order when it's no longer pending
        if order == self.target_order and order.status in [order.Completed, order.Canceled, order.Rejected]:
            self.target_order = None  # Reset the target order when it's no longer pending

    def notify_trade(self, trade):
        if trade.isclosed:
            self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}')
