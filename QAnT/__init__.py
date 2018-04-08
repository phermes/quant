import datetime as dt
import os
import sqlite3

import numpy as np
import pandas as pd

# from QAnT.algorithm import algo
from QAnT.data_downloader import keyratios as download_keyratios
from QAnT.data_downloader import quarterly_report
from QAnT.fundamentals import fundamentals
from QAnT.fundamentals import fundamentals
from QAnT.output import logging, plotting

# classes and methods for indices
from QAnT.quotes import index_quote
from QAnT.quotes import quotes
from QAnT.risk import get_returns_var_vola
from QAnT.tools import get_datetime, convert_sql_date_to_datetime_date

# TOM : added global variable to contain directory of the location of the databases, should
# actual go in the initialization of the class and in the object instantiation

databasedir = '/Users/tommertens/PycharmProjects/quant/'


class time:
    """
    Make the analysis for today a special case of backtesting
    """

    def _assign_pointintime(self, day):
        # self.bdate = the backtesting date
        self.day = day
        self.bdate = self.quote.date[0] + dt.timedelta(days=self.day)
        self.quote = self.quote[self.quote.date < self.bdate]

        if self.bdate.month < 4:
            _max_keyratio_year = self.bdate.year - 2
        else:
            _max_keyratio_year = self.bdate.year - 1

        self.keyratios = self.keyratios[self.keyratios['year'] < _max_keyratio_year]


class checks:
    """
    Perform initialization tests:
     - if output directory non-existent create it

    """

    @staticmethod
    def _perform_initialization_checks():
        for directory in ["output/"]:
            if not os.path.isdir(directory):
                os.makedirs(directory)


class searchtools:
    def __init__(self):
        pass

    def find_by_name(self, *args):
        """
        Find stocks by their name.
        Usage:
            >>> s = stocks()
            >>> s.find_by_name('DAIMLER', 'BAY', 'COCA')
        """

        outstr = "|".join(args)
        results = self.list[self.list['name'].str.contains(outstr)]

        return results


class stock(quotes, fundamentals, algo, time, logging, plotting, quarterly_report, download_keyratios, searchtools,
            checks):
    """
    Base class to handle stocks
    """

    def __init__(self, verbose=False, isin=None, debug=False, control=False):
        super().__init__()
        #  TOM : inits were outside __init__ scope - updated to include them in __init__
        self.list = pd.DataFrame()  # init with empty dataframe
        # self.index = 0
        # self.name = None
        # self.isin = None
        # self.ticker = None
        # self._ticker_ms = None
        # self.branch = None
        # self.benchmark = None
        # self._fn_link = None

        self.get_stocklist()  # load the list of stocks from the database
        self._initialize_algo()  # set all parameters of the algorithm to the default value
        self._control = control  # boolean to specify if stock class is called from the control script
        self.verbose = verbose  # activate verbose mode
        self.debug = debug  # debug mode
        self._end = False  # boolean specifying if we are at the end of the list
        self.isin = isin  # Stock isin
        self._type = "stock"  # type of financial product analyzed

        # create sub-directories required for the operation
        # -> checks class
        self._perform_initialization_checks()

        # select starting point
        if self.isin is None:
            self.switch_index(0)
        else:
            self.switch_isin(self.isin)

        self._update_tables()  # initialize the tables

    def get_stocklist(self):
        """
        Load the stock list from the database in self.list
        """
        cnx = sqlite3.connect(databasedir + 'database/stocks_main.db')
        xetra = pd.read_sql("SELECT name, isin, ticker_YF, ticker_MS, branch, benchmark, finanzen_net FROM xetra;", cnx)

        self.list = xetra

    def reset(self):
        """
        Reset variables except verbose, end, list and debug
        """

        dic = vars(self)

        for i in dic.keys():
            if i in ['verbose', 'end', 'list', 'debug', 'control']:
                continue
            dic[i] = None

    def switch_isin(self, isin):
        """
        Switch to a stock based on the ISIN
        """

        self.reset()

        df = self.list[self.list['isin'] == isin]

        self.index = df.index[0]
        self.name, self.isin, self.ticker, self._ticker_ms, self.branch, self.benchmark, self._fn_link = np.array(df)[0]

        # TOM -> output.py -> class : logging
        self.debug_message("New stock selected - {0}".format(self.name))
        self.debug_message("ISIN & Ticker:       {0}, {1}".format(self.isin, self.ticker))

        self._update_tables()

        # TOM : algorithm.py -> class : algo
        self._initialize_algo()

    def switch_index(self, index):
        """
        Switch to stock based on the index
        """

        self.reset()
        self.index = index

        df = self.list[self.list.index == index]

        self.name, self.isin, self.ticker, self._ticker_ms, self.branch, self.benchmark, self._fn_link = np.array(df)[0]

        # TOM : algorithm.py -> class : algo
        self._initialize_algo()

    def switch_next(self):
        """
        Switch to the next stock
        """

        try:
            self.switch_index(self.index + 1)
            self._update_tables()

            # TOM : algorithm.py -> class : algo
            self._initialize_algo()

        except IndexError:
            self.switch_index(self.index - 1)
            self._update_tables()

            # TOM : algorithm.py -> class : algo
            self._initialize_algo()
            self._end = True

    def _update_tables(self):
        """
        update table in database
        """

        self._get_keyratios()

        try:
            # check if there are stored quotes
            self._read_stored_quotes()

        except:
            if self.verbose:
                print('No stored quotes were found.')
            return

        # if worked the length of the quotes table should be larger than zero
        if len(self.quote) > 0:
            # TOM: where does the 30 come from, does this absolutely needs to be hardcoded?
            # TOM: risk.py -> function call
            self.volatility = get_returns_var_vola(self.quote, 30)

    def update_time(self, day):
        """
        This function resets the data such that only the data known at the
        selected moment in time is available.
        """

        self._update_tables()

        # TOM : -> class : time
        self._assign_pointintime(day)


class Index(logging, index_quote):
    """
    Base class to handle indices
    """

    # TOM : removed ticker - not used
    def __init__(self, verbose=False, debug=False):
        self.verbose = verbose
        self.debug = debug
        self._type = "index"

        # TOM : were not initialized in __init__
        self.name = None
        self.country = None
        self.ticker = None
        self.index = None

        self._get_indexlist()

        # initialize with the first index in list
        self._list_generator = self.list.iterrows()
        self.switch_next()

        # get the quotes
        try:
            # TOM : quotes.py -> class: index_quote
            self._read_stored_quotes()
        except:
            # TOM : maybe do some extra handling or print message?
            pass

    def switch_next(self):
        """
        switch to next index in the database
        """

        row = next(self._list_generator)[1]

        self.name, self.country, self.ticker = row['name'], row['country'], row['ticker']

    def switch_to_ticker(self, ticker):
        """
        Switch to ticker symbol
        :param ticker:
        """

        df = self.list[self.list['ticker'] == ticker]

        self.index = df.index[0]
        self.name, self.country, self.ticker = np.array(df)[0]

    def _get_indexlist(self):
        """
        Load the stock list from the database
        """

        cnx = sqlite3.connect(databasedir + 'database/stocks_main.db')
        xetra = pd.read_sql("SELECT name, country, ticker FROM indices;", cnx)
        self.list = xetra
