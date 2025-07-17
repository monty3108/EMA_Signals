# Order Feed Starter



from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from Alice_Module import *
import datetime  # For datetime objects
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])
import pandas as pd
from Trade_Live import Order, Trade
from Logger_Module import my_logger, logging
# import logging
from pya3 import *
from Gen_Functions import *
import OrderStatusFeed as o
from Order_Manager import *
import pickle


from enum import Enum
import config



import threading
#import time
from queue import Queue
from My_Logger import setup_logger, LogLevel
logger = setup_logger(logger_name="Feed Starter", log_level=LogLevel.INFO, log_to_console=config.print_logger)
# Queue to store notifications in order
notification_queue = Queue()

# Function to process notifications from the queue
def notification_worker():
    while True:
        notification = notification_queue.get()  # Get the next notification from the queue
        if notification is None:  # If None is received, stop the worker
            break
        
        # Sending the notification
        #my_logger(data_to_log=notification, bot=True) 
        my_logger(notification) 
          
        notification_queue.task_done()

# Function to generate and send notifications
def log(message):
    # Put the notification into the queue for the worker to process
    notification_queue.put(message)

# Start a background thread that continuously processes notifications
worker_thread = threading.Thread(target=notification_worker)
worker_thread.daemon = True  # Ensures the worker thread exits when the main program exits
worker_thread.start()






# Constants


WEBSOCKET_START_TIME = datetime.datetime.strptime("08:30:00", "%H:%M:%S").time()
SESSION_START_TIME = datetime.datetime.strptime("09:14:59", "%H:%M:%S").time()
SESSION_END_TIME = datetime.datetime.strptime("15:30:00", "%H:%M:%S").time()

time_cons = []
time_cons.append(f"Websocket Start Time: {WEBSOCKET_START_TIME}")
time_cons.append(f"Session Start Time: {SESSION_START_TIME}")
time_cons.append(f"Session End Time; {SESSION_END_TIME}")



# Generating Session ID
if config.alice is None:
    logger.info("alice object is None. Calling get_session_id()")
    get_session_id()
    # session_id_generate()
    logging.debug(f'alice obj after calling:{config.alice} ')   

# Setting alice value from config file alice obj
alice = config.alice

try:
    o.start_order_feed_websocket()
    while True:
        # On Session Over @1530hrs break while loop
        if get_time() >= SESSION_END_TIME:
            log('Order Feed Session End')
            logging.info('Session End')
            break        

except Exception as e:
        text = f"Error: {e}"
        log(text)
        logging.exception(text)    

notification_queue.join()  # Block until all notifications are processed
print("All notifications sent, exiting.")
# Stop the worker thread
notification_queue.put(None)  # Send a signal to stop the worker
worker_thread.join()  # Wait for the worker thread to finish
