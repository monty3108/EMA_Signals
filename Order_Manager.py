from enum import Enum

from pya3.alicebluepy import *
import config
from Gen_Functions import read_pkl, round_nearest, write_pkl, get_time
from Logger_Module import my_logger
import threading
import time

alice = None
from My_Logger import setup_logger, LogLevel
logger = setup_logger(logger_name="Order Manager", log_level=LogLevel.INFO, log_to_console=config.print_logger)

def send_order(transaction_type,
               inst,
               qty,
               order_type,
               product_type,
               price,
               trigger_price=None,
               stop_loss=None,
               square_off=None,
               is_amo=False,
               order_tag='order1'):
    fn = 'send_order'
    global alice
    alice= config.alice
    price = round_nearest(price)
    logger.info(f'Placing Order of Inst: {inst}')
    response = alice.place_order(transaction_type=transaction_type,
                                 instrument=inst,
                                 quantity=qty,
                                 order_type=order_type,
                                 product_type=product_type, 
                                 price=price,
                                 trigger_price=trigger_price,
                                 stop_loss=stop_loss,
                                 square_off=square_off,
                                 trailing_sl=None,
                                 is_amo=is_amo,
                                 order_tag=order_tag)
    print(f'Order response: {response}') 

    if response['stat'] == 'Ok':
        order_id = response['NOrdNo']
        text = f'Order placed successfully. Order id: {order_id} '
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.info(text)
        return order_id
    else:
        text = f'Order Response: {response}'
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.info(text)


def check_order_status():
    fn = "check_order_status"
    from config import order_status_dict
    # print(f'order_status_dict: {order_status_dict}')


    try:
        global alice
        alice = config.alice

        if get_time() < config.SESSION_START_TIME:
            reset_order_files()

        response = alice.get_order_history('')

        if type(response) is list:
            pending_order_id = []
            complete_order_id = []
            rejected_order_id = []
            order_id_response = {}
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
                order_id_response[response[i]['Nstordno']] = {"stat": response[i]['stat'],
                                                              "order_status": response[i]['Status'],
                                                              "rejection_reason": response[i]['RejReason'],
                                                              "trading_symbol": response[i]['Trsym'],
                                                              "quantity": response[i]['Qty'],
                                                              "average_price": float(response[i]['Avgprc']),
                                                              "trigger_price": response[i]['Trgprc'],
                                                              "product": response[i]['Pcode'],
                                                              "price": response[i]['Prc'],
                                                              "transaction_type": response[i]['Trantype']
                                                              }

                # updating global variable order_status_dict with latest status
                order_status_dict[response[i]['Nstordno']] = {
                    'record_date': get_time(),
                    'status': response[i]['Status'],
                    'tsym': response[i]['Trsym'],
                    'price': response[i]['Prc'],
                    'rejreason': response[i]['RejReason']
                }

                if response[i]['Status'] == 'rejected':
                    rej_order_id = response[i]['Nstordno']
                    print(f'Rejected order id: {rej_order_id}')
                    rejected_order_id.append(rej_order_id)
                    rej_order_ids = read_pkl(file_path='pkl_obj/rejected_order_id.pkl')

                    if rej_order_ids is None:  # execute if None
                        logger.info("no order id or file found")
                    else:
                        print(f'Rejected order ids: {rej_order_ids}')
                        # restricting recurring rejection notifications
                        if rej_order_id in rej_order_ids:
                            print(f'msg already sent for {rej_order_id}')
                        else:
                            text = f"{rej_order_id} rejected: {response[i]['RejReason']}"
                            print(text)
                            my_logger(data_to_log=text, fn=fn, bot=True)
                            logger.warning(text)
                            print(f"rejected_order_id: {rejected_order_id}")

            # writing variables to file
            files = [rejected_order_id, order_id_response]
            file_path = [config.path_rejected_order_id, config.path_order_id_response]
            for i in range(len(files)):
                write_pkl(obj=files[i], file_path=file_path[i])

            logger.info("check_order_status executed.")
        else:
            text = f'get order history response: {response}'
            my_logger(data_to_log=text, fn=fn, bot=True)
            logger.info(text)


    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.exception(text)


def is_pending(order_id):
    fn = 'is_pending'

    try:
        from config import order_status_dict
        status = order_status_dict[order_id]['status'].lower()
        return status == 'open' or status == 'trigger_pending' or status == 'trigger pending' or status == 'pending'
        # check_order_status()
        # pending_order_id = read_pkl(file_path='pkl_obj/pending_order_id.pkl')
        # return order_id in pending_order_id
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.exception(text)


def is_complete(order_id):
    fn = 'is_complete'
    try:
        from config import order_status_dict
        status = order_status_dict[order_id]['status'].lower()
        return status == 'complete'
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.exception(text)


def get_price(order_id):
    fn = 'get_price'
    try:
        if is_complete(order_id):
            order_id_response = read_pkl(file_path=config.path_order_id_response)
            return order_id_response[order_id]['average_price']
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.exception(text)


def is_rejected(order_id):
    fn = 'is_rejected'

    try:
        from config import order_status_dict
        status = order_status_dict[order_id]['status'].lower()
        return status == 'reject' or status == 'rejected'
        # check_order_status()
        # pending_order_id = read_pkl(file_path='pkl_obj/pending_order_id.pkl')
        # return order_id in pending_order_id
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.exception(text)

def reset_order_files():
    """func to clear files: order_id_response.pkl and rejected_order_id.pkl"""
    fn = 'reset_order_files'
    try:
        file_path = [config.path_rejected_order_id, config.path_order_id_response]
        for i in range(len(file_path)):
            x = read_pkl(file_path=file_path[i])
            x.clear()
            write_pkl(obj=x, file_path=file_path[i])
        logger.info(f"cleared order files: {file_path}")
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=True)
        logger.exception(text)

def has_pending_orders():
    fn = 'has_pending_orders'
    try:
        pending_status_list = ['open', 'pending', 'trigger_pending', 'trigger pending']
        orders = config.order_status_dict
        for order in orders:
            if orders[order]['status'].lower() in pending_status_list:
                return True

        return False
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=False)
        logger.exception(text)


def pending_checks() :
    fn = 'pending_checks'
    check_order_status()

    try:
        if get_time() < config.SESSION_END_TIME:
            if len(read_pkl(config.path_order_id_response))>0:

                if has_pending_orders():
                    logger.debug('Pending orders. Running after 1 mins')
                    threading.Timer(60, pending_checks).start()  # Schedule the task to run after 1 minute
                else:
                    logger.debug('No pending orders. Running after 5 mins')
                    threading.Timer(300, pending_checks).start()  # Schedule the task to run after 5 minutes
            else:
                logger.info('Nil Orders. Running after 30 mins')
                threading.Timer(1800, pending_checks).start()
        else:
            logger.info("Exiting pending checks.....")
    except Exception as e:
        text = f"Error: {e}"
        my_logger(data_to_log=text, fn=fn, bot=False)
        logger.exception(text)
