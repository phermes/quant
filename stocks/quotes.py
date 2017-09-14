import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt

from stocks.tools import get_datetime, convert_sql_date_to_datetime_date

class quotes:
    def __init__(self):
        pass

    def _get_lastprice(self):
        self.lastquote = self.quote[self.quote.date==self.quote.date.max()].close.values
    
    def _get_quote(self):
        # load the entries for the current stock from the database
        cnx         = sqlite3.connect('database/stocks_quotes.db')
        sql         = pd.read_sql_query("SELECT * FROM quotes WHERE isin = '{0}';".format(self.isin), cnx)
        cnx.close()    
        if sql is not None:
            sql['date'] = sql.date.apply(convert_sql_date_to_datetime_date)
        self.quote  = sql
        
        self._get_lastprice()
        
    def get_highwatermark(self):
        '''Calculate the high watermark of the dataset'''
        hw = []
        mark = 0
        for i,j in self.quote.iterrows():
            if j['close']>mark and j['close']>0:
                mark = j['close']
            hw.append(mark)
        self.quote = self.quote.assign(highwatermark=pd.Series(hw))
