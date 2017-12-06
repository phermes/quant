import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt

from QAnT.quotes import quotes
from QAnT.fundamentals import fundamentals
from QAnT.fundamentals import fundamentals
from QAnT.algorithm import algo
from QAnT.output import logging, plotting
from QAnT.tools import get_datetime, convert_sql_date_to_datetime_date
from QAnT.data_downloader import quarterly_report
from QAnT.data_downloader import keyratios as download_keyratios

# classes and methods for indices
from QAnT.quotes import index_quote

from QAnT.risk import get_returns_var_vola


class time:
    '''Make the analysis for today a special case of backtesting'''
    
    def _assign_pointintime(self, day):
        # self.bdate = the backtesting date
        self.day      = day
        self.bdate    = self.quote.date[0] + dt.timedelta(days=self.day)
        self.quote    = self.quote[self.quote.date < self.bdate]
        
        if self.bdate.month<4:
            _max_keyratio_year = self.bdate.year - 2 
        else:
            _max_keyratio_year = self.bdate.year - 1

        self.keyratios = self.keyratios[self.keyratios['year'] < _max_keyratio_year]

class stock(quotes,fundamentals,algo,time,logging,plotting,quarterly_report, download_keyratios):
    '''Base class to handle stocks'''

    def __init__(self,verbose=False,isin=None,debug=False):
        self.get_stocklist()          # load the list of stocks from the database
        self._initialize_algo()       # set all parameters of the algorithm to the default value
        self.verbose = verbose
        self.debug   = debug
        self.end     = False    
        self.isin    = isin
        self._type   = "stock"

        if self.isin is None:
            self.switch_index(0)          # select starting point
        else:
            self.switch_isin(self.isin)

        self._update_tables()             # initialize the tables

    def get_stocklist(self):
        '''Load the stock list from the database'''
        cnx        = sqlite3.connect('database/stocks_main.db')
        xetra      = pd.read_sql("SELECT name, isin, ticker_YF, ticker_MS, branch, benchmark, finanzen_net FROM xetra;", cnx)
        self.list  = xetra 

    def reset(self):
        '''Reset variables except verbose, end, list and debug'''
        dic = vars(self)
        for i in dic.keys():
            if i in ['verbose', 'end', 'list', 'debug']:
                continue
            dic[i] = None
        
    def switch_isin(self,isin):
        '''Switch to a stock based on the ISIN'''
        self.reset()
        df         = self.list[self.list['isin']==isin]
        self.index = df.index[0]
        self.name, self.isin, self.ticker, self._ticker_ms, self.branch, self.benchmark, self._fn_link = np.array(df)[0]
        self.debug_message("Switched to new stock: {0}".format(self.name))
        self.debug_message("ISIN & Ticker:         {0}, {1}".format(self.isin, self.ticker))
        self._update_tables()
        self._initialize_algo()
        
    def switch_index(self,index):
        '''Switch to stock based on the index'''
        self.reset()        
        self.index = index
        df         = self.list[self.list.index==index]
        self.name, self.isin, self.ticker, self.ticker_ms, self.branch, self.benchmark, self._fn_link = np.array(df)[0]
        self._initialize_algo()
        
    def switch_next(self):
        '''Switch to the next stock'''
        try:
            self.switch_index(self.index+1)
            self._update_tables()
            self._initialize_algo()
        except IndexError:
            self.switch_index(self.index-1)
            self._update_tables()
            self._initialize_algo()            
            self.end = True            

    def _update_tables(self):
        self._get_keyratios()
        try:
            self._read_stored_quotes()
        except:
            return
        if len(self.quote)>0:
            self.volatility = get_returns_var_vola(self.quote, 30)
        
    def update_time(self,day):
        '''This function resets the data such that only the data known at the 
        selected moment in time is available. '''
        self._update_tables()        
        self._assign_pointintime(day)


class Index(logging,index_quote):
    '''Base class to handle stocks'''

    def __init__(self,ticker=None,verbose=False,debug=False):
        self.verbose = verbose
        self.debug   = debug
        self._type   = "index"

        self._get_indexlist()
        
        # initialize with the first index in list
        self._list_generator = self.list.iterrows()
        self.switch_next()

        # get the quotes
        try:
            self._read_stored_quotes()
        except:
            pass

    def switch_next(self):
        row = next(self._list_generator)[1]    
        self.name, self.country, self.ticker = row['name'], row['country'], row['ticker']        

    def switch_to_ticker(self,ticker):
        df         = self.list[self.list['ticker']==ticker]
        self.index = df.index[0]
        self.name, self.country, self.ticker = np.array(df)[0]

    def _get_indexlist(self):
        '''Load the stock list from the database'''
        cnx        = sqlite3.connect('database/stocks_main.db')
        xetra      = pd.read_sql("SELECT name, country, ticker FROM indices;", cnx)
        self.list  = xetra 

        

    # def get_stocklist(self):
    #     '''Load the stock list from the database'''
    #     cnx        = sqlite3.connect('database/stocks_main.db')
    #     xetra      = pd.read_sql("SELECT name, isin, ticker_YF,branch, finanzen_net FROM xetra;", cnx)
    #     self.list  = xetra 

    # def reset(self):
    #     '''Reset variables except verbose, end, list and debug'''
    #     dic = vars(self)
    #     for i in dic.keys():
    #         if i in ['verbose', 'end', 'list', 'debug']:
    #             continue
    #         dic[i] = None
        
    # def switch_isin(self,isin):
    #     '''Switch to a stock based on the ISIN'''
    #     self.reset()
    #     df         = self.list[self.list['isin']==isin]
    #     self.index = df.index[0]
    #     self.name, self.isin, self.ticker, self.branch, self._fn_link = np.array(df)[0]
    #     self.log_message("Switched to new stock: {0}".format(self.name))
    #     self.log_message("ISIN & Ticker:         {0}, {1}".format(self.isin, self.ticker))
    #     self._update_tables()
    #     self._initialize_algo()
        
    # def switch_index(self,index):
    #     '''Switch to stock based on the index'''
    #     self.reset()        
    #     self.index = index
    #     df         = self.list[self.list.index==index]
    #     self.name, self.isin, self.ticker, self.branch, self._fn_link = np.array(df)[0]
    #     self._initialize_algo()
        
    # def switch_next(self):
    #     '''Switch to the next stock'''
    #     try:
    #         self.switch_index(self.index+1)
    #         self._update_tables()
    #         self._initialize_algo()
    #     except IndexError:
    #         self.end = True            

    # def _update_tables(self):
    #     self._get_keyratios()
    #     self._read_stored_quotes()
        
    # def update_time(self,day):
    #     '''This function resets the data such that only the data known at the 
    #     selected moment in time is available. '''
    #     self._update_tables()        
    #     self._assign_pointintime(day)

