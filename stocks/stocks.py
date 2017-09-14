import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt

from stocks.quotes import quotes
from stocks.fundamentals import fundamentals
from stocks.algorithm import algo
from stocks.output import logging, plotting


def get_datetime(inputobj):
    return dt.datetime.date(inputobj)


def convert_sql_date_to_datetime_date(string):
    return dt.datetime.strptime(string,'%Y-%m-%d').date()


class stocks(quotes,fundamentals,algo,time,logging,plotting):
    def __init__(self,stocklist,index=0,ISIN=None,verbose=False):
        
        self._initialize_algo()
        
        self.list    = stocklist
        self.verbose = verbose

        if ISIN:
            self.switch_isin(ISIN)
        else:
            self.switch_index(index)
        self._update_tables()
            
    def switch_isin(self,isin):
        df         = self.list[self.list.ISIN==isin]
        self.index = df.index[0]
        self.name, self.isin, self.ticker = np.array(df)[0]
        self._update_tables()
        self._initialize_algo()
        
        
    def switch_index(self,index):
        self.index = index
        df         = self.list[self.list.index==index]
        self.name, self.isin, self.ticker = np.array(df)[0]
        self._initialize_algo()
        
    def switch_next(self):
        try:
            self.switch_index(self.index+1)
            self._update_tables()
            self._initialize_algo()
            
        except IndexError:
            pass

    def _update_tables(self):
        self._get_keyratios()
        self._get_quote()
        
    def update_time(self,day):
        '''This function resets the data such that only the data known at the 
        selected moment in time is available. '''
        self._update_tables()        
        self._assign_pointintime(day)


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
