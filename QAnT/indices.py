import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt
import pandas_datareader.data as web
import quandl as ql

from stocks.tools import get_datetime, convert_sql_date_to_datetime_date


class benchmarks:
    def __init__(self):
        self.indices = ['FTMIB.MDD', '^GDAXI', '^GSPC', '^SSMI', '^IBEX', '^FTSE', '^ISEQ',
       '^FCHI', '^BFX', '^N225', '^GSPTSE', 'XWD.TO', '^AEX', '^AORD',
       '^ATX', '000001.SS', 'OBXBID.OL', '^OMXS30', 'SAGD.AT', 'PSI20.LS',
       'INHSI.PA', 'WIG.PA', '^OMXC20', '^MERV', '^BVSP', '^OMXH25',
       '^NQCZ']

    def _read_stored_quotes(self,stock_index):
        '''Load the quotes for the current stock from the database.'''
        cnx          = sqlite3.connect('database/stocks_quotes.db')
        quote_saved  = pd.read_sql_query("SELECT * FROM indices WHERE ticker = '{0}';".format(stock_index), cnx)
        cnx.close()
        
        # convert date strings to datetime objects
        quote_saved['date'] = quote_saved.date.apply(convert_sql_date_to_datetime_date)
        
        self.index_quote_saved = quote_saved
        self.index_quote       = quote_saved

    def _save_in_sql(self):
        '''Save the quote to save in the database'''
        if self.quote_to_save is None:
            print("Could not find quote to save")
            return
    
        cnx         = sqlite3.connect('database/stocks_quotes.db')
        self.quote_to_save.to_sql('indices',cnx,if_exists='append',index=False)
        cnx.close()    
        
        print("Successfully saved {0} entries in quote database".format(len(self.quote_to_save)))
    
    def _extract_unsaved_rows(self):
        '''Identify the rows in the new '''
        
        # get the quotes stored in the database
        self._read_stored_quotes()
        
        # compare the dates between downloaded and saved dates
        s1       = self.quote_downloaded['date']
        s2       = self.index_quote_saved['date']
        newdates = pd.Series(np.setdiff1d(s1.values,s2.values))
        newdates = newdates.values

        # extract the lines to save
        quote_to_save = self.quote_downloaded[self.quote_downloaded['date'].isin(newdates)]

        print("Found {0} quotes to save".format(len(quote_to_save)))

        self.quote_to_save = quote_to_save

    
    def _prepare_raw_quote_for_saving(self,raw_quote,ticker):
        '''Prepare the quote dataframe for saving'''
        # find columns which are not in the dataframe and replace them with zeros
        for _col in ['Date','Open','High','Low','Close','Volume']:
            if _col not in raw_quote.columns:
                raw_quote = raw_quote.assign(**{_col:pd.Series(np.zeros(len(raw_quote))).values})
                
        self.quote_downloaded = raw_quote
                
        # add columns with the name and isin of the stock
        self.quote_downloaded = self.quote_downloaded.assign(ticker=pd.Series([ticker for _ in range(len(self.quote_downloaded))]))
                
        # re-order the elements
        self.quote_downloaded = self.quote_downloaded[['Date','ticker','Open','High','Low','Close','Volume']]
        
        # re-name the elements
        self.quote_downloaded.columns = ['date','ticker','open',
                                          'high','low','close','volume']
        
        # put date in the correct format
        self.quote_downloaded['date'] = self.quote_downloaded['date'].apply(get_datetime)
        
        # Remove NaN entries
        self.quote_downloaded = self.quote_downloaded.dropna(thresh=6)
        
        # produce the saveable object
        self._extract_unsaved_rows()

    def _download_quote_yahoo(self,ticker):
        '''Download the latest stock quote from yahoo finance'''
   
        # assign the start and end date
        start = dt.datetime(1900, 1, 1)
        end   = dt.datetime.today()    
        
        # prepare and perform the query
        load_successful = False
        for i in range(1,6):     # try multiple times
            try:
                quote = web.DataReader("{0}".format(ticker), 'yahoo', start, end)  # query
                load_successful = True
                print('Successfully loaded quote for {0} from yahoo'.format(ticker))
                break
            except:
                if verbose:
                    print('Trials to load {0} quote: {1}/5'.format("{0}".format(ticker),i))
                continue

        if not load_successful:
        	print("Quote not found")
            return

        
        # move date from index to column
        if 'Date' not in quote.index:
            quote.reset_index(level=0, inplace=True)

        quote = quote[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        # self.quote_yahoo_raw       = longest_quote                                       # raw quote
        self._prepare_raw_quote_for_saving(quote,ticker)        
        self._save_in_sql()