from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from pya3 import *
from Gen_Functions import is_holiday_today, create_dir
from Alice_Module import *

# constants from config files
import config
from My_Logger import setup_logger, LogLevel
logger = setup_logger(logger_name="Trade Log", log_level=LogLevel.INFO, log_to_console=config.print_logger)

create_dir(config.dir_name)

# Exit if today is holiday
is_holiday_today()

# Generating Session ID
if config.alice is None:
    logger.info("alice object is None. Calling get_session_id()")
    get_session_id()
    # session_id_generate()
    logger.debug(f'alice obj after calling:{config.alice} ')

# logging balance on csv. Try to maintain only one file
log_trade_book()
log_all_logs()
log_strategy_book()

# Sending required logs to Telegram
try:
    # docs_to_send = ["app_logs.txt", "data.txt", "logs/trade_log.csv",  "logs/balance.csv"]
    docs_to_send = [config.path_trade_log,
                    config.path_strategy_log,
                    config.path_balance,
                    'logs/get_netwise_positions.json',
                    'logs/get_holding_positions.json',
                    'logs/get_daywise_positions.json',
                    'logs/get_order_history.json',
                    'logs/get_balance.json',
                    'logs/get_trade_book.json']
    bot_token = '5398501864:AAFEn7ljDrKOVkXzhWX4P_khX9Xk-E8FicE'
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    bot_chat_id = ['5162043562']
    for item in docs_to_send:
        document = open(item, "rb")
        response = requests.post(url, data={'chat_id': bot_chat_id}, files={'document': document})
        # logger.info(response.json())
        logger.info(f"{item} sent to Bot.")
except Exception as e:
    text = f"Error: {e}"
    logger.exception(text)