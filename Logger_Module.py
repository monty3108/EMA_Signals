#!/usr/bin/env python
# coding: utf-8
""" Logger Module for Live & Paper Trading """

import requests
import datetime
import logging
import threading
import pytz 

def get_time():
    current_time = datetime.datetime.now(pytz.timezone('ASIA/KOLKATA')).time()
    return current_time


def my_telegram_bot(bot_message):
    test_str = bot_message
    # initializing mapping dictionary
    map_dict = {
        '.': '\\.',
        '!': '\\!',
        '}': '\\}',
        '{': '\\{',
        '|': '\\|',
        '=': '\\=',
        '-': '\\-',
        '+': '\\+',
        '(': '\\(',
        ')': '\\)',
        '_': '\\_'
    }
    # generator expression to construct vals
    # join to get string
    final_message = ''.join(idx if idx not in map_dict else map_dict[idx]
                            for idx in test_str)
    # printing result
    bot_token = '5398501864:AAFEn7ljDrKOVkXzhWX4P_khX9Xk-E8FicE'
    # bot_chat_id = ['5162043562', '5392684854', '1702956168', '5602254212']
    # Rakesh::  '1362917754' Ravi: 5392684854 Maurya: 1702956168 rawat : '5602254212'
    # Gajendra: '748274003'
    bot_chat_id = ['5162043562'] # Mine Chat id
    # bot_chat_id = ['5162043562', '748274003']  
    for receiver in bot_chat_id:
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + receiver + \
                    '&parse_mode=MarkdownV2&text=' + final_message
        response = requests.get(send_text)
        if response.status_code != 200:
            print(f"Failed to send message: {response.text}")
        # print(response.json())
    return


def my_logger(data_to_log, fn="nf_buy", sym="my_logger", bot=True, bt=False):
    try:
        if bt is False:
            # t = datetime.datetime.now().time().isoformat("seconds")
            t = get_time().isoformat("seconds")
            text = f"{t}: {data_to_log} ({fn}|{sym})"
        else:
            text = f"{data_to_log} ({sym}|{fn})"

        with open("data.txt", mode='a') as logfile:
            logfile.write(f"{text}\n")
        if bot:
            my_telegram_bot(text)
    except Exception as e:
        # t = datetime.datetime.now().time().isoformat("seconds")
        t = get_time().isoformat("seconds")
        text = f"{t}: mylogger Error: {e}"
        with open("data.txt", mode='a') as logfile:
            logfile.write(f"\n{text}")


# FORMAT = '%(asctime)s- %(levelname)s: %(message)s | file:%(filename)s|func:%(funcName)s'
FORMAT = '%(asctime)s- %(levelname)s: %(message)s | file:%(filename)s|func:%(funcName)s|line: %(lineno)d|thread: %(threadName)s'
FORMAT_BT = '%(levelname)s: %(message)s | file:%(filename)s|func:%(funcName)s|line: %(lineno)d|thread: %(threadName)s'

# FOR WRITING IN FILE COMMENT OUT FOLLOWING CODE
logging.basicConfig(filename='app_logs.txt',
                    format=FORMAT,
                    datefmt='%d-%b-%y %H:%M:%S')
# logging.basicConfig(
#     format=FORMAT,
#     datefmt='%d-%b-%y %H:%M:%S')

# Creating a logging object
logger = logging.getLogger()

# comment out the following code for writing in log file
logger.setLevel(logging.INFO)

# DEBUG: The lowest level, used for detailed diagnostic information.
# INFO: Used for confirming that things are working as expected.
# WARNING: Indicates something unexpected happened or could happen.
# ERROR: Records an error that prevented the software from performing a function.
# CRITICAL: The highest level, for severe problems hindering program functions.

# import threading


class LoggerThread(threading.Thread):

    def __init__(self, text, bot=False, f_name="ND", fn="Logger"):
        threading.Thread.__init__(self)
        self.text = text
        self.bot_message = bot
        # self.dt = datetime.datetime.now().time().isoformat("seconds")
        self.dt = get_time().isoformat("seconds")
        self.file_name = f_name
        self.func = fn
        self.log_msg = f"{self.dt}: {self.text} ({self.func} | {self.file_name})"
        self.file = "data.txt"

    def run(self):
        try:
            with open(self.file, 'a') as f:
                # text = f"{self.dt}: {self.text} ({self.func} | {self.file_name})
                f.write(self.log_msg + '\n')
                # print(self.log_msg)
            if self.bot_message is True:
                my_telegram_bot(self.log_msg)
        except Exception as e:
            # t = datetime.datetime.now().time().isoformat("seconds")
            text = f"{self.dt}: LoggerThread Error: {e}"
            with open("data.txt", mode='a') as f:
                f.write(f"\n{text}")
            my_telegram_bot(text)


# # Create a new logger thread and start it
# log_thread = LoggerThread('Some text to log', 'data.txt')
# log_thread.start()
