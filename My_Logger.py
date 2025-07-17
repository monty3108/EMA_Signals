"""Module to write logs using logging module"""

import logging
import logging.handlers
from enum import Enum
import config
import os


class LogLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

def create_logging_dir(dir_name: list):
    """ create dir
    args:
    dir_name is to be passed as a list of directory """
    for directory in dir_name:
        if not os.path.exists(directory):
            os.makedirs(directory)

def setup_logger(logger_name='my_logger', log_name="app_logs", log_level=LogLevel.INFO, log_to_console=True):
    """
    Sets up a logger for the application.

    Args:
        logger_name (str): The base name for the logger. Defaults to 'my_logger'
        log_name (str): The base name for the log file. Defaults to 'app_logs'.
        log_level (LogLevel): The logging level to set. Defaults to LogLevel.INFO.
        log_to_console (bool): Toggle for printing logs to the console. Defaults to True.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # create required directories
    create_logging_dir(config.dir_name)
    # Gen_Functions.create_dir(config.dir_name)
    # Create logger instance

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level.value)
    logger.propagate = False  # Prevent propagation to root logger

    # Prevent duplicate handlers if logger is reused
    if not logger.handlers:
        # Create formatter
        log_format = '%(asctime)s- %(levelname)s| %(name)s: %(message)s | file:%(filename)s|func:%(funcName)s|line: %(lineno)d|thread: %(threadName)s'
            # ('%(asctime)s- %(levelname)s| %(name)s: %(message)s '
            #           '|file:%(filename)s|func:%(funcName)s|line: %(lineno)d|thread: %(threadName)s')
        formatter = logging.Formatter(log_format, datefmt="%d-%m-%Y %H:%M:%S")

        # File handler setup
        # date_str = datetime.now().strftime("%d-%m-%Y")
        # file_handler = logging.handlers.TimedRotatingFileHandler(
        #     config.logger_file_name, when="midnight", backupCount=7
        # )
        file_handler = logging.FileHandler(config.logger_file_name)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level.value)
        logger.addHandler(file_handler)

        # Console handler setup (optional)
        if log_to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.setLevel(log_level.value)
            logger.addHandler(console_handler)



    return logger

# # Example Usage:
if __name__ == "__main__":
    # Initialize logger
    logger = setup_logger(log_level=LogLevel.DEBUG)

    # Log sample messages
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")