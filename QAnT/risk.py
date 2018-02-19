import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt
import pandas_datareader.data as web


class risk:
    def __init__(self):
        pass
    def get_value_at_risk(self,significance=5):
        '''Returns the value at risk for the indicated significance level'''
        var = np.percentile(self.quote['retrel'].dropna(),significance)

        self.value_at_risk = var

        self.debug_message("Value at Risk calculated {0:4.2f}".format(var))

        df = pd.DataFrame([[self.name,self.isin,self.value_at_risk]], columns=['name','isin','var'])

        cnx         = sqlite3.connect('database/stocks_risk.db')
        mydata      = cnx.execute("DELETE FROM value_at_risk WHERE isin=?", (self.isin,))
        cnx.commit()

        df.to_sql('value_at_risk', cnx, if_exists='append', index=False)
        cnx.close() 

def get_datetime(inputobj):
    return dt.datetime.date(inputobj)

def convert_sql_date_to_datetime_date(string):
    return dt.datetime.strptime(string,'%Y-%m-%d').date()

def _assign_colnames(stock_object):
    '''Helper function to assign the colnames for merge_quotes'''
    df   = stock_object.quote
    name = df['name'].iloc[0]                                                # use name of the first column
    name = name[0:6]                                                         # use only the first six characters of the name
    df   = df[['date','close','retrel']]    
    df.columns = ['date','{0}'.format(name), 'retrel_{0}'.format(name)]   
    df = df[['date','retrel_{0}'.format(name)]] 
    return df

def merge_quotes(*args):
    '''Merge multiple quote dataframes to allow studies of correlation and outperformance'''
    newdf = _assign_colnames(args[0])
    for k in range(1,len(args)):
        dftoadd = _assign_colnames(args[k])
        newdf = pd.merge(newdf, dftoadd, how='inner',on='date')
    return newdf

# def _get_return_individual(quote,daysfromnow_lower, daysfromnow_upper):
#     '''Returns the mean return, the 1% value at risk and the volatility'''
#     print("Function _get_return_individual is depreciated. use get_value_at_risk")
#     date_lower  = max(quote['date']) - dt.timedelta(daysfromnow_lower)
#     date_upper  = max(quote['date']) - dt.timedelta(daysfromnow_upper)
#     quote_month = quote[(quote['date']>date_lower) & (quote['date']<date_upper)]
#     r           = np.diff(quote_month['close'])/quote_month['close'][1:]     # return
#     r_mean      = r.mean()
#     r_VaR       = r.quantile(0.01)
#     r_vola      = r.std()
#     return date_lower, (r_mean, r_VaR*(-1), r_vola)

# def get_returns_var_vola(quote, timestep):
#     '''Returns the return, variance and volatility.
#     timestep: the time over which the quantities should be calculated
#     '''
#     print("Function get_returns_var_vola is depreciated")
#     n = 0
#     output = []
#     while True:
#         date, (r, var, vola) = _get_return_individual(quote, (n+1)*timestep, n*timestep)
#         date = date + dt.timedelta(int(timestep/2))
#         if any((np.isnan(r), np.isnan(var), np.isnan(vola))):
#             break
#         output.append([date, r, var, vola])
#         n+=1
#     output = pd.DataFrame(output, columns=['date','return','var','vola'])
#     return output
