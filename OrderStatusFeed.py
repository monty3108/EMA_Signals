#OrderStatusFeed.py
#Aliceblue
#To be run after session generated

import websocket
import requests
import json
import threading
import time
import config
from Gen_Functions import create_dir, file_exist, read_pkl, write_pkl
from Logger_Module import my_logger
import datetime
from Order_Manager import check_order_status
from queue import Queue
from My_Logger import setup_logger, LogLevel
logger = setup_logger(logger_name="Order Feed", log_level=LogLevel.INFO, log_to_console=config.print_logger)

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


def generate_token():
    logger.info('generating order feed token') 
    alice = config.alice
    if alice is None:
        logger.critical('alice var is None')
    """Generates an access token using API key and secret."""
    base_url ="https://ant.aliceblueonline.com/order-notify/" 
    token_url="ws/createWsToken"
    ws_url = "websocket" 
    Url = base_url + token_url
    res = alice._request(Url, "GET" )
    logger.info(f'generate token response: {res} ') 
    if res['status'] == 'Ok' :
        token_data = res
        access_token = token_data['result'][0]['orderToken']
        print(type(access_token)) 
        payload = {
        "orderToken" : access_token, 
        "userid": "AB154186" 
        } 
        datas = json.dumps(payload) 
        return datas
    else:
        logger.error(f"Error in token generation: {res.json()}")
        return None


def on_message(ws1, message):
    """Handles incoming WebSocket messages."""
    data = json.loads(message)
    new_msg = json.dumps(data, indent=4)
    print('new message recd(order status feed):')
    print(new_msg)
    log(new_msg) 
    logger.info(new_msg) 
    if 't' in data:
        manage_order_status(data) 

def on_error(ws1, error):
    """Handles WebSocket errors."""
    log(f"Error: {error}")

def on_close(ws1, close_status_code, close_msg):
    """Handles WebSocket closing."""
    logger.info("Order WebSocket closed")
    log("order feed closed") 
    start_order_feed_websocket()

def on_open(ws1):
    """Handles WebSocket connection opening."""
    logger.info("WebSocket connection established")

    # update config.order_status_dict by using check_order_status in case orders are already sent
    check_order_status()

    ws1.send(generate_token() )
    
    # Send heartbeat every minute to keep connection alive
    def send_heartbeat():
        while True:
            h = { 
              "heartbeat": "h", 
              "userId":"AB154186"
            }
            res = ws1.send(json.dumps(h))
            logger.debug(f"Heartbeat sent. Res: {res}")
            print(f"Heartbeat sent. Res: {res}")
            time.sleep(50)
    #starting heartbeat thread
    threading.Thread(target=send_heartbeat, daemon=True).start()

def start_order_feed_websocket():
    """Establishes WebSocket connection."""
    
    logger.info("Establishes WebSocket connection.... ")
    thread = None
    create_dir = ['pkl_obj']
    WEB_SOCKET_URL =  "wss://ant.aliceblueonline.com/order-notify/websocket"
    
    #headers = token
    
    ws1 = websocket.WebSocketApp(WEB_SOCKET_URL,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    ws1.on_open = on_open
    thread = threading.Thread(target=ws1.run_forever) 
    thread.daemon = True
    thread.start()
    # ws1.run_forever()
    
def manage_order_status(order_msg):
    """func to maintain order status with order id""" 
    from config import order_status_dict

    check_order_status() # func from Order_Manager.py to update order status variables
    order_id = order_msg['norenordno']
    file_path = 'pkl_obj/orderstatusfeed.pkl'
    today_date = datetime.date.today()
    # update order status in config.order_status_dict
    order_status_dict[order_msg['norenordno']] = {
        'record_date': today_date,
        'status': order_msg['status'],
        'tsym': order_msg['tsym'],
        'price': order_msg['prc'],
        'rejreason': ''
    }
    logger.info(f'order_status_dict updated for order id : {order_id}')
    # if order is rejected, update rejection reason
    if order_status_dict[order_id] ['status']=='REJECTED' :
        order_status_dict[order_id] ['rejreason']=order_msg['rejreason']
        logger.info(f'rejection reason updated for order id: {order_id}')
    # for logging
    msg= {order_id: order_status_dict[order_msg['norenordno']]}
    logger.info(msg)

    print(f"order_status_dict: {order_status_dict}")


def del_old_records() :
    file_path = 'pkl_obj/orderstatusfeed.pkl'   
    today_date = datetime.date.today()
    records = read_pkl(file_path) 
    ids_to_del = [] 
    
    for record in records:
        print(record) 
        if records[record]['record_date'] < today_date:
            ids_to_del.append(record) 
            print(f'{record} deleted') 
           
    for id in ids_to_del:
             del records[id] 
            
    print('for exited') 
    print(records) 
    write_pkl(file_path=file_path, obj=records) 


