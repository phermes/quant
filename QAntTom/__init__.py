# Authors : Pascal Hermes - Tom Mertens
# Version : 1.
# Last modified : 02-04-2018

# standard packages
import numpy as np
import pandas as pd
import datetime as dt
import os
import time
import sys


# database imports
import sqlite3



# logging imports
import logging
import logging.config
from colorlog import ColoredFormatter

# global package constants
# TODO : this should be provided by user somehow
databasedir = '/Users/tommertens/PycharmProjects/quant/'
filenamelog = '../output/quantdebug.log'
logreset = True

databasequotes = databasedir + 'database/stocks_quotes.db'

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Purpose :
#       - setting up correct file system
#       - check if necessary data is available and loadable
#       - creating logger instance for logging of
# setup logging
# -------------
# basic setup :
# add filemode = "w" to overwrite
# logging.basicConfig(filename=filenamelog, level=logging.INFO)

# using config file - but didn't figure out yet how to load colors yet
# log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logging.conf')
# logging.config.fileConfig(log_file_path, disable_existing_loggers=True)
# the method below is simple and straightforward - loggers can easily be used in other files
# To print the log configuration file
# with open(log_file_path,'r') as f:
#     print(f.read())


# https://stackoverflow.com/questions/29087297/is-there-a-way-to-change-the-filemode-for-a-logger-object-that-is-not-configured
def create_logger():
    """
    Creates a logger for the whole program.
    Method is loaded at bottom of this file such that a logger is immediately available
    :return: a logger containing a stream (info level) and file handler (debug level)
    """

    # create logger for "Sample App"
    logger = logging.getLogger('quantlog')
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filenamelog)

    # set the FileHandler mode
    #   - w : overwrite log file
    #   - a : keep old log file and append new logs to that file

    if logreset:
        logmode = 'w'
    else:
        logmode = 'a'

    fh = logging.FileHandler(log_file_path, mode=logmode)
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    logformat = '[%(log_color)s%(asctime)s] %(log_color)s%(levelname)8s --- %(log_color)s%(message)s ' \
                + '(%(filename)s:%(lineno)s)'
    formatter = ColoredFormatter(logformat, datefmt='%Y-%m-%d %H:%M:%S')
    formatterfile = logging.Formatter('[%(asctime)s] %(levelname)8s --- %(message)s ' + '(%(filename)s:%(lineno)s)',
                                      datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatterfile)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger


def perform_initialization_checks():
    """
    Perform initialization tests:
     - output directory
     - database directory
        + quotes database

    """
    # output directory
    logger.debug('Checking for output directory...')
    for directory in [databasedir + "output/"]:
        if not os.path.isdir(directory):
            logger.warning('Directory does not exist - creating ...')
            os.makedirs(directory)
        else:
            logger.debug('Directory {} found...'.format(directory))

    # database directory
    logger.debug('Checking for database directory...')
    for directory in [databasedir + "database/"]:
        if not os.path.isdir(directory):
            logger.critical('Directory does not exist - not data present')
            raise FileNotFoundError('Directory database not found - missing databases')
        else:
            logger.debug('Directory {} found...'.format(directory))

    # databases present
    logger.debug('Checking for databases ...')
    for file in [databasequotes]:
        if not os.path.isfile(file):
            logger.critical('Database {} not found.'.format(file))
            raise FileNotFoundError('Database {} not found.'.format(file))
        else:
            logger.debug('Database {} found...'.format(file))


def get_timestamp():
    """
    Returns datetime formatted as string
    :return: date time string
    """
    ts = time.time()
    return dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def get_datetime(inputobj):
    """
    Transform to date
    :param inputobj:
    :return: date
    """
    return dt.datetime.date(inputobj)


def convert_sql_date_to_datetime_date(string):
    """
    Convert an SQL date to datetime date
    :param string:
    :return: date string YYYY-mm-dd
    """
    return dt.datetime.strptime(string, '%Y-%m-%d').date()


# create logger


logger = create_logger()

logger.debug('Performing initialization tests...')
perform_initialization_checks()
logger.debug('Initializations tests done !!!')
