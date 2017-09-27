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

def get_datetime(inputobj):
    return dt.datetime.date(inputobj)

def convert_sql_date_to_datetime_date(string):
    return dt.datetime.strptime(string,'%Y-%m-%d').date()



class quotes:
    def __init__(self):
        pass
    
    def _get_lastprice(self):
        '''Get the last price of the '''
        self.lastquote = self.quote[self.quote.date==self.quote.date.max()].close.values
        self.quote_cur = self.quote.currency.values[0]

    def _find_splits(self, quote):
        '''Find historic splits from quote'''

        relchange   = quote['Close'].diff()/quote['Close']
        splits      = (relchange[relchange<-0.5]*(-1)+ 1).round()

        if len(splits)>20:
            self.error_message("Found suspiciously many potential splits ({0}), escaping".format(len(splits)))
            return quote

        for i,splitdate in enumerate(quote.loc[splits.index, 'Date']):
            # print("Splitdate: {0}".format(splitdate))
            quote.loc[quote.Date < 
                        splitdate,'Close'] = quote.loc[quote.Date < 
                                                         splitdate,'Close']/np.array(splits)[i]  
        return quote
    
    def _read_stored_quotes(self):
        '''Load the quotes for the current stock from the database.'''
        if self.verbose:
            self.log_message("Reading saved quote for stock {0}".format(self.name))
        cnx          = sqlite3.connect('database/stocks_quotes.db')
        quote_saved  = pd.read_sql_query("SELECT * FROM quotes WHERE ISIN = '{0}';".format(self.isin), cnx)
        cnx.close()
        
        # convert date strings to datetime objects
        quote_saved['date'] = quote_saved.date.apply(convert_sql_date_to_datetime_date)
        
        self.quote_saved = quote_saved
        self.quote       = quote_saved

    def _calculate_volatility(self):
        '''Calculate the daily and yearly volatility of the stock'''
        pass

    def analyze_quote(self):
        '''Do some basic analysis with the downloaded quote'''
        self._read_stored_quotes()
        
    def _yahoo_get_longest_quote(self,quotes): 
        '''Return the longest quote downloaded from yahoo with the different keys'''
        self.log_message('Finding longest quote')
        longest_quote = max(quotes, key=lambda k: len(quotes[k]))
        return quotes[longest_quote], longest_quote
    
    def _save_in_sql(self):
        '''Save the quote to save in the database'''
        if self.quote_to_save is None:
            if self.verbose:
                self.error_message("Could not find quote to save")
            return
    
        cnx         = sqlite3.connect('database/stocks_quotes.db')
        self.quote_to_save.to_sql('quotes',cnx,if_exists='append',index=False)
        cnx.close()    
        
        if self.verbose:
            self.log_message("Successfully saved {0} entries in quote database".format(len(self.quote_to_save)))
    
    def _extract_unsaved_rows(self):
        '''Identify the rows in the new '''
        
        # get the quotes stored in the database
        self._read_stored_quotes()
        
        # compare the dates between downloaded and saved dates
        s1       = self.quote_downloaded['date']
        s2       = self.quote_saved['date']
        newdates = pd.Series(np.setdiff1d(s1.values,s2.values))
        newdates = newdates.values

        # extract the lines to save
        quote_to_save = self.quote_downloaded[self.quote_downloaded['date'].isin(newdates)]

        self.log_message("Found {0} quotes to save".format(len(quote_to_save)))

        self.quote_to_save = quote_to_save

        self.log_message("currency of quote_to_save: {0}".format(self.quote_to_save['currency'].unique()))
        self.log_message("currency of quote_saved  : {0}".format(self.quote_saved['currency'].unique()))

        # check if the quote to save and saved quote have the same currency
        if len(self.quote_to_save)!=0 and len(self.quote_saved)!=0:
            self.log_message("Compare currencies of saved and downloaded quote")
            if self.quote_saved['currency'].unique() != self.quote_to_save['currency'].unique():
                self.log_message("Cannot save new quote, wrong currency compared to saved quote")
                self.quote_to_save=None
            else: 
                self.log_message("Saved quote and quote to save have the same currency: {0}".format(self.quote_to_save['currency'].unique()))
        elif len(self.quote_to_save)==0 and len(self.quote_saved)!=0:
            self.log_message("Nothing to save")
        elif len(self.quote_to_save)!=0 and len(self.quote_saved)==0:    
            self.log_message("No quote saved, adding new entry")
    
    def _prepare_raw_quote_for_saving(self,raw_quote):
        '''Prepare the quote dataframe for saving'''
        # find columns which are not in the dataframe and replace them with zeros
        for _col in ['Date','Open','High','Low','Close','Volume']:
            if _col not in raw_quote.columns:
                raw_quote = raw_quote.assign(**{_col:pd.Series(np.zeros(len(raw_quote))).values})
                
        self.quote_downloaded = raw_quote
                
        # add columns with the name and isin of the stock
        self.quote_downloaded = self.quote_downloaded.assign(name=pd.Series([self.name for _ in range(len(self.quote_downloaded))]))
        self.quote_downloaded = self.quote_downloaded.assign(isin=pd.Series([self.isin for _ in range(len(self.quote_downloaded))]))
        self.quote_downloaded = self.quote_downloaded.assign(currency=pd.Series([self.quote_currency for _ in range(len(self.quote_downloaded))]))
        self.quote_downloaded = self.quote_downloaded.assign(exchange=pd.Series([self.quote_exchange for _ in range(len(self.quote_downloaded))]))
                
        # re-order the elements
        self.quote_downloaded = self.quote_downloaded[['Date','name','isin','exchange',
                                                        'currency','Open','High','Low','Close','Volume']]
        
        # re-name the elements
        self.quote_downloaded.columns = ['date','name','isin','exchange','currency','open',
                                          'high','low','close','volume']
        
        # put date in the correct format
        self.quote_downloaded['date'] = self.quote_downloaded['date'].apply(get_datetime)
        
        # Remove NaN entries
        self.quote_downloaded = self.quote_downloaded.dropna(thresh=6)
        
        # produce the saveable object
        self._extract_unsaved_rows()
        
    def _download_quote_yahoo(self):
        '''Download the latest stock quote from yahoo finance'''
        ISIN, ticker = self.isin, self.ticker
        if self.financial is not None:
            self.log_message("Found different ticker symbol for yahoo finance")
            self.log_message("Morningstar: {0}, Yahoo: {1}".format(self.ticker, self.financial))
            ticker = self.financial

        verbose      = self.verbose
        key          = ISIN[0:2]

        # assign the stock exchanges to use as a function of the country
        exchange       = {}
        exchange['AT'] = ['.VI','.DE', '.F', '.SG','.BE','.DU','.HM','.HA','.MU']       # austria                                    
        exchange['DE'] = ['.DE', '.F', '.SG','.BE','.DU','.HM','.HA','.MU']             # germany
        exchange['US'] = ['']                                                                 # US
        exchange['CH'] = ['.VX','.SW']                                                        # switzerland
        exchange['GB'] = ['.L', '.IL', '.F']                                                               # 
        exchange['AU'] = ['.AX','.DE', '.F',  '.SG','.BE']
        exchange['FR'] = ['.PA', '.NX', '.DE', '.F', '.SG','.BE','.DU','.HM','.HA','.MU'] # france
        exchange['JP'] = ['.DE', '.F',  '.SG','.BE']

        # assign the currency as a function of the stock exchange used
        currency = {'.DE': 'EUR', 
                    ''   : 'USD',  
                    '.F' : 'EUR', 
                    '.SG': 'EUR', 
                    '.PA': 'EUR', 
                    '.BE': 'EUR',
                    '.BM': 'EUR',
                    '.DU': 'EUR',
                    '.HM': 'EUR',
                    '.HA': 'EUR',
                    '.MU': 'EUR',
                    '.VX': 'CHF',  
                    '.SW': 'CHF',
                    '.AX': 'AUD',
                    '.NX': 'EUR',
                    '.L' : 'GBP',
                    '.IL': 'GBP'
                   }

        # assign the start and end date
        start = dt.datetime(1900, 1, 1)
        end   = dt.datetime.today()    
        
        if key not in exchange.keys():
            if self.verbose:
                print("Key not supported: {0}".format(key))
            return
        # prepare and perform the query
        quotes = {}
        load_successful = False
        for ex in exchange[key]:     # loop over all exchanges
            for i in range(1,6):     # try multiple times
                try:
                    quotes[ex] = web.DataReader("{0}{1}".format(ticker,ex), 'yahoo', start, end)  # query
                    load_successful = True
                    # some output message when successful
                    if verbose:
                        self.log_message('Successfully loaded quote for {0} from yahoo, exchange {1}'.format(ticker,ex))
                    break
                except:
                    if verbose:
                        print('Trials to load {0} quote: {1}/5'.format("{0}{1}".format(ticker,ex),i))
                    continue

        if not load_successful:
            if verbose:
                self.log_message("No quote found.")
            return

        # find the longest dataframe and prepare it for saving
        self.quotes_yahoo          = quotes
        longest_quote, longest_key = self._yahoo_get_longest_quote(self.quotes_yahoo)
        
        # move date from index to column
        if 'Date' not in longest_quote.index:
            longest_quote.reset_index(level=0, inplace=True)

        longest_quote = longest_quote[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        longest_quote = self._find_splits(longest_quote)
        
        self.log_message('Check stock info at https://finance.yahoo.com/quote/{0}{1}?p={0}{1}'.format(ticker, longest_key))

        self.quote_yahoo_raw       = longest_quote                                       # raw quote
        self.quote_currency        = currency[longest_key]                               # currency of the quote
        self.quote_exchange        = "Y {0}{1}".format(ticker,longest_key)                                         
        self._prepare_raw_quote_for_saving(self.quote_yahoo_raw)
        
        self._save_in_sql()




# class quotes:
#     def __init__(self):
#         pass

#     def _get_lastprice(self):
#         self.lastquote = self.quote[self.quote.date==self.quote.date.max()].close.values

#     def _extract_unsaved_rows(self):
#         # give back the rows of the loaded stock quote which are not in the database

#         # load the entries for the current stock from the database
#         cnx         = sqlite3.connect('database/stocks_quotes.db')
#         sql         = pd.read_sql_query("SELECT * FROM quotes;", cnx)
#         cnx.close()
        
#         sql['date'] = sql.date.apply(convert_sql_date_to_datetime_date)

#         # remove the intersection of the saved and the loaded quote
#         df = pd.merge(sql, self.quote, how='right', indicator=True)
#         df = df[df['_merge']!='both']
#         df = df.drop('_merge', 1)
#         if self.verbose:
#             print("Stock {0}, found {1} rows to add".format(self.name,len(df)))
#         return df

#     def _get_quote_quandl(self,callkey=None):
#         isin, ticker = self.isin, self.ticker
        
#         if isin[0:2]=='DE':
#             prefix = "FSE/"
#             suffix = "_X"
#             if callkey is not None:
#                 key = callkey
#                 self.quote_exchange = "Q {0}".format(key)
#             else:
#                 key = "{0}{1}{2}".format(prefix,ticker,suffix)
#                 self.quote_exchange = "Q {0}".format(key)
#             df  = ql.get(key, authtoken="CkyBkkkbzfh3ePfDF_ip")
#             df  = df[['Open','High','Low','Close','Traded Volume']]
#             df.columns = ['Open','High','Low','Close','Volume'] 
#             self.quote_currency = 'EUR'
            
#         elif isin[0:2]=='CH':
#             if callkey is not None:
#                 key = callkey
#                 self.quote_exchange = "Q {0}".format(key)
#             else:            
#                 key = "SIX/{0}CHF1".format(self.isin)
#                 self.quote_exchange = "Q {0}".format(key)
#             self.quote_currency = 'CHF'            
#             df  = ql.get(key, authtoken="CkyBkkkbzfh3ePfDF_ip")            
#             df.columns = ['Close','Volume']  
            
#         elif isin[0:2]=='US':
#             df = ql.get_table('WIKI/PRICES',
#                               ticker=ticker, 
#                               paginate=True, 
#                               authtoken="CkyBkkkbzfh3ePfDF_ip")
            
#             self.quote_exchange = "Q {0}".format(ticker)
#             self.quote_currency = 'USD'            
#             df = df[['date','open','high','low','close','volume']]
#             df.columns = ['Date','Open','High','Low','Close','Volume']
            
#         if 'Date' not in df.index:
#             df.reset_index(level=0, inplace=True)
            
#         return df
        
#     def _get_quote_google(self):
#         isin   = self.isin
#         ticker = self.ticker
        
#         if isin[0:2]=='DE':
#             prefix="FRA:"
#             self.quote_currency = 'EUR'            
#         elif isin[0:2]=='CH':
#             self.quote_currency = 'EUR'            
#             prefix="SIX"
#         elif isin[0:2]=='US':
#             self.quote_currency = 'USD'
#             prefix='NYSE'
#         else:
#             return


#         start = dt.datetime(1900, 1, 1)
#         end   = dt.datetime.today()
            
#         key = "{0}{1}".format(prefix,ticker)
#         self.quote_exchange = "G {0}".format(key)        
#         df  = web.DataReader(key, 'google', start, end)
        
#         if 'Date' not in df.index:
#             df.reset_index(level=0, inplace=True)        
        
#         return df
        
#     def _get_quote_yahoo(self,callkey=None,verbose=False):
#         ISIN, ticker = self.isin, self.ticker
        
#         key      = ISIN[0:2]

#         exchange       = {}
#         exchange['AT'] = ['.DE', '.F', '.SG','.BE']
#         exchange['DE'] = ['.DE', '.F', '.SG','.BE']
#         exchange['US'] = ['']
#         exchange['CH'] = ['.VX','.F','.DE']
#         exchange['GB'] = ['.F']
#         exchange['AU'] = ['.DE', '.F',  '.SG','.BE']
#         exchange['FR'] = ['.PA', '.DE', '.F', '.SG','.BE']
#         exchange['JP'] = ['.DE', '.F',  '.SG','.BE']

#         currency = {'.DE': 'EUR', '':'USD',  '.F':'EUR', '.VX':'CHF', '.SG':'EUR', '.PA':'EUR', '.BE':'EUR'}

#         start = dt.datetime(1900, 1, 1)
#         end   = dt.datetime.today()    


#         if callkey is not None:
#             for i in range(1,6): # try multiple times, 
#                 try:
#                     df         = web.DataReader(callkey.split()[1], 'yahoo', start, end)
#                     ex         = callkey.split()[1]
#                     if verbose:
#                         print('Successfully loaded quote for {0} from yahoo, exchange {1}'.format(ISIN,ex))
#                     break
#                 except:
#                     if verbose:
#                         print('Trials to load {0} quote: {1}/5'.format("{0}{1}".format(ticker,e),i))
#                         continue

#             if len(df)==0:
#                 return None
            
#             self.quote_exchange = callkey
            
#             if 'Date' not in df.index:
#                 df.reset_index(level=0, inplace=True)

#             df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

#             return df

#         # if quote not in database
        
#         df1     = {}
#         dstring = {}
#         for e in exchange[key]:
#             for i in range(1,6): # try multiple times, 
#                 try:
#                     dstring[e] ="Y {0}{1}".format(ticker,e)
#                     df1[e]     = web.DataReader("{0}{1}".format(ticker,e), 'yahoo', start, end)
#                     ex         = e
#                     if verbose:
#                         print('Successfully loaded quote for {0} from yahoo, exchange {1}'.format(ISIN,ex))
#                     break
#                 except:
#                     if verbose:
#                         print('Trials to load {0} quote: {1}/5'.format("{0}{1}".format(ticker,e),i))
#                     continue

#         df = []
#         ex = ''
#         for e in df1.keys():
#             if len(df1[e])>len(df):
#                 df   = df1[e]
#                 dstr = dstring[e]
#                 ex   = e

#         self.quote_exchange = callkey

#         if len(df)==0:
#             return None

#         if 'Date' not in df.index:
#             df.reset_index(level=0, inplace=True)

#         df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

#         return df
    
#     def _find_splits(self):
#         relchange   = self.quote.Close.diff()/self.quote.Close
#         self.splits = (relchange[relchange<-0.5]*(-1)+ 1).round()

#         for i,splitdate in enumerate(self.quote.loc[self.splits.index].Date):
#             self.quote.loc[self.quote.Date < 
#                         splitdate,'Close'] = self.quote.loc[self.quote.Date < 
#                                                          splitdate,'Close']/np.array(self.splits)[i]        
#     def _prepare_for_saving(self):
#         if self.quote is None:
#             return
        
#         # find columns which are not in the dataframe and replace them with zeros
#         for _col in ['Date','Open','High','Low','Close','Volume']:
#             if _col not in self.quote.columns:
#                 self.quote = self.quote.assign(**{_col:pd.Series(np.zeros(len(self.quote))).values})
                
#         # add columns with the name and isin of the stock
#         self.quote = self.quote.assign(name=pd.Series([self.name for _ in range(len(self.quote))]))
#         self.quote = self.quote.assign(isin=pd.Series([self.isin for _ in range(len(self.quote))]))
#         self.quote = self.quote.assign(currency=pd.Series([self.quote_currency for _ in range(len(self.quote))]))
#         self.quote = self.quote.assign(exchange=pd.Series([self.quote_exchange for _ in range(len(self.quote))]))
                
#         # re-order the elements
#         self.quote = self.quote[['Date','name','isin','exchange',
#                                  'currency','Open','High','Low','Close','Volume']]
        
#         # re-name the elements
#         self.quote.columns = ['date','name','isin','exchange','currency','open',
#                               'high','low','close','volume']
        
#         # put date in the correct format
#         self.quote['date'] = self.quote['date'].apply(get_datetime)
        
#         # produce the saveable object
#         self.quote_to_save = self._extract_unsaved_rows()
        
#     def _save_in_sql(self):
#         if len(self.quote_to_save)==0:
#             return
        
#         cnx         = sqlite3.connect('database/stocks_quotes.db')
#         self.quote_to_save.to_sql('quotes',cnx,if_exists='append',index=False)
#         cnx.close()
        
        
#     def get_quote(self, find_splits=True, download=False, save=False, methods='all', verbose=False):
#         '''Populates the dataframe self.quote, depending on the option selected.'''

#         cnx         = sqlite3.connect('database/stocks_quotes.db')
#         old_quote   = pd.read_sql_query("SELECT * FROM quotes WHERE ISIN = '{0}';".format(self.isin), cnx)
#         if len(old_quote)!=0:
#             self.quote_currency = old_quote['currency'].iloc[0]
#         cnx.close()

        
#         if not download:
#             # load the entries for the current stock from the database, don't download from web
#             print("User selected not to download the data. Reading from local database.")
#             self.quote         = old_quote
#             self.quote['date'] = self.quote.date.apply(convert_sql_date_to_datetime_date)
#             self._get_lastprice()            
#             return


#         if len(old_quote)==0:
                        
#             success_quandl = False
#             success_yahoo  = False

#             # load quote from yahoo
#             if methods in ['yahoo', 'all']:
#                 try:
#                     quote_yahoo   = self._get_quote_yahoo()
#                     if quote_yahoo is not None:
#                         success_yahoo = True
#                 except:
#                     pass 
        
#                 # load quote from quandl
#                 if methods in ['quandl', 'all']:
#                     try:
#                         quote_quandl   = self._get_quote_quandl()
#                         if quote_quandl is not None:
#                             success_quandl = True
#                     except:
#                         pass

#                     # if neither of them worked load from google
#                 if not success_quandl and not success_yahoo and methods in ['google', 'all']:
#                     quote         = self._get_quote_google()
#                 else:
#                     if success_quandl and success_yahoo:
#                         if len(quote_quandl) > len(quote_yahoo):
#                             quote = quote_quandl
#                             if verbose:
#                                 print("Data from quandl more detailed: Using data from quandl")
#                         else:
#                             quote = quote_yahoo
#                             if verbose:
#                                 print("Data from yahoo more detailed: Using data from yahoo")
#                     elif success_quandl and not success_yahoo:
#                         quote = quote_quandl
#                         if verbose:
#                             print("Could not load yahoo data: using data from quandl")
#                     elif not success_quandl and success_yahoo:
#                         quote = quote_yahoo
#                         if verbose:
#                             print("Using data from yahoo")
#         else:
#             key      = old_quote['exchange'].iloc[-1]
            
#             if key.split()[0] == 'Q':
#                 quote = self._get_quote_quandl(callkey=key.split()[1])
#             elif key.split()[0] == 'Y':
#                 quote = self._get_quote_yahoo(callkey=key.split()[1])
        
#         # assign the quote 
#         self.quote = quote
                            
#         if not find_splits:
#             return
#         try:
#             self._find_splits()
#         except AttributeError:
#             self.quote = None

#         if save:
#             self._prepare_for_saving()
#             self._save_in_sql()
