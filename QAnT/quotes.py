import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt
import pandas_datareader.data as web
# import fix_yahoo_finance as yf
import os
import time


# yf.pdr_override()


def get_datetime(inputobj):
    return dt.datetime.date(inputobj)


def convert_sql_date_to_datetime_date(string):
    return dt.datetime.strptime(string, '%Y-%m-%d').date()


class index_quote:
    def __init__(self):
        pass

    def _download(self):
        '''Downloads the quotes for all indices in the database'''

        start = dt.datetime(1900, 1, 1)
        end = dt.datetime.today()
        import pandas_datareader.data as web

        quotes = {}
        while True:
            load_successful = False

            for i in range(1, 6):  # try multiple times
                try:
                    # if True:
                    quote_df = web.DataReader("{0}".format(self.ticker), 'yahoo', start, end)  # query
                    quote_df = self._prepare_downloaded_quotes_for_saving(quote_df)
                    quotes[self.ticker] = pd.DataFrame(quote_df)
                    load_successful = True

                    # some output message when successful
                    self.log_message('Successfully loaded index quote for {0} from yahoo'.format(self.ticker))
                    break
                except:
                    self.debug_message('Attempts to load {0} quote: {1}/5'.format(self.ticker, i))
                    continue

            if not load_successful:
                self.log_message("Quote {0} could not be downloaded".format(self.ticker))
                try:
                    self.switch_next()
                except StopIteration:
                    break
                continue

            # save the quote in sql database
            self.debug_message("Finding quotes to save")
            try:
                quote_to_save = self._extract_unsaved_rows(self._quote_saved[self.ticker], quote_df)
            except (KeyError, AttributeError):
                quote_to_save = quote_df

            self.debug_message("Saving {0} line in sql database".format(len(quote_to_save)))
            cnx = sqlite3.connect(self._path_to_sql_file)
            quote_to_save.to_sql('indices', cnx, if_exists='append', index=False)
            cnx.close()

            # switch to the next ticker in list
            try:
                self.switch_next()
            except StopIteration:
                break
                # break

    def _prepare_downloaded_quotes_for_saving(self, quote_df):
        '''Prepare the downloaded quotes for saving
           
           - Fills up missing columns with zeros
           - Adds new columns including the index name and ticker symbol
        '''

        if 'Date' not in quote_df.index:
            quote_df.reset_index(level=0, inplace=True)

        # find columns which are not in the dataframe and replace them with zeros
        self.debug_message("Assigning Columns")
        for _col in ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']:
            if _col not in quote_df.columns:
                quote_df = quote_df.assign(**{_col: pd.Series(np.zeros(len(quote_df))).values})

        # add columns with the name and ticker of the stock
        self.debug_message("_prepare_downloaded_quotes_for_saving: Assigning name and ticker columns")
        quote_df = quote_df.assign(name=pd.Series([self.name for _ in range(len(quote_df))]))
        quote_df = quote_df.assign(ticker=pd.Series([self.ticker for _ in range(len(quote_df))]))

        # # re-order the elements
        quote_df = quote_df[['Date', 'name', 'ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]

        # # re-name the elements
        quote_df.columns = ['date', 'name', 'ticker', 'open', 'high', 'low', 'close', 'volume']

        # # put date in the correct format
        quote_df['date'] = quote_df['date'].apply(get_datetime)

        # # Remove NaN entries
        quote_df = quote_df.dropna(thresh=6)

        return quote_df
        # # produce the saveable object
        # self._extract_unsaved_rows()

    def _read_stored_quotes(self):

        self._path_to_sql_file = "/Users/tommertens/PycharmProjects/quant/database/stocks_quotes.db"

        '''Load the quotes for the current stock from the database.'''
        print('reading stored quotes')
        self.debug_message("Reading saved quote for all indices")
        # load from the database
        cnx = sqlite3.connect(self._path_to_sql_file)
        quote_saved = pd.read_sql_query("SELECT * FROM indices;", cnx)
        quote_saved['date'] = quote_saved.date.apply(convert_sql_date_to_datetime_date)
        cnx.close()

        self.debug_message("Found {0} quotes in database".format(len(quote_saved)))
        self.debug_message("Splitting quote_saved into dictionary")

        quote_saved_dict = {}
        for k in np.unique(quote_saved['ticker']):
            quote_saved_dict[k] = quote_saved[quote_saved['ticker'] == k]

        self._quote_saved = quote_saved_dict
        self.quote = quote_saved
        print(self.quote)

    def _extract_unsaved_rows(self, saved, downloaded):
        '''Identify the rows in the downloaded quote df that shall be saved.'''

        # compare the dates between downloaded and saved dates
        s1 = saved['date']
        s2 = downloaded['date']

        # dates for which a quote has not yet been saved in the database
        newdates = np.setdiff1d(s2.values, s1.values)

        # extract the lines to save
        quote_to_save = downloaded[downloaded['date'].isin(newdates)]

        self.debug_message("Found {0} quotes to save".format(len(quote_to_save)))
        return quote_to_save


class quotes:
    def __init__(self):
        pass

    def _get_lastprice(self):
        '''Get the last price of the '''
        self.lastquote = self.quote[self.quote.date == self.quote.date.max()].close.values
        self.quote_cur = self.quote.currency.values[0]

    def _get_earlier_price(self, daysago):
        '''Get the price in the past (in days)'''
        self.lastquote = self.quote[self.quote.date == self.quote.date.max()].close.values
        self.quote_cur = self.quote.currency.values[0]

    def _find_splits(self, quote):
        '''Find historic splits from quote'''
        relchange = quote['Close'].diff() / quote['Close']
        splits = (relchange[relchange < -0.5] * (-1) + 1).round()

        if len(splits) > 20:
            self.error_message("Found suspiciously many potential splits ({0}), escaping".format(len(splits)))
            return quote

        for i, splitdate in enumerate(quote.loc[splits.index, 'Date']):
            # print("Splitdate: {0}".format(splitdate))
            quote.loc[quote.Date <
                      splitdate, 'Close'] = quote.loc[quote.Date <
                                                      splitdate, 'Close'] / np.array(splits)[i]
        return quote

    def _read_stored_quotes(self):
        '''Load the quotes for the current stock from the database.'''

        self.debug_message("Reading saved quote")

        cnx = sqlite3.connect('/Users/tommertens/PycharmProjects/quant/database/stocks_quotes.db')
        quote_saved = pd.read_sql_query("SELECT * FROM quotes WHERE ISIN = '{0}';".format(self.isin), cnx)
        cnx.close()

        self.log_message("Found {0} quotes".format(len(quote_saved)))

        # convert date strings to datetime objects
        quote_saved['date'] = quote_saved.date.apply(convert_sql_date_to_datetime_date)

        self._quote_saved = quote_saved
        self.quote = quote_saved

    def _calculate_volatility(self):
        '''Calculate the daily and yearly volatility of the stock'''
        self.dailychange = self.quote['close'].diff().shift(-1) / self.quote['close'][:-1]
        self.volatility_day = self.dailychange.std()
        self.volatility_year = self.volatility_day * np.sqrt(252)

    def analyze_quote(self):
        '''Do some basic analysis with the downloaded quote'''
        self._read_stored_quotes()
        self._calculate_volatility()

    def _yahoo_get_longest_quote(self, quotes):
        '''Return the longest quote downloaded from yahoo with the different keys'''
        self.debug_message('Finding longest quote')
        longest_quote = max(quotes, key=lambda k: len(quotes[k]))
        return quotes[longest_quote], longest_quote

    def _save_in_sql(self):
        '''Save the quote to save in the database'''
        if self.quote_to_save is None:
            if self.verbose:
                self.error_message("Could not find quote to save")
            return

        cnx = sqlite3.connect('/Users/tommertens/PycharmProjects/quant/database/stocks_quotes.db')
        self.quote_to_save.to_sql('quotes', cnx, if_exists='append', index=False)
        cnx.close()

        self.log_message("Successfully saved {0} entries in quote database".format(len(self.quote_to_save)))

    def _extract_unsaved_rows(self):
        '''Identify the rows in the new '''

        # get the quotes stored in the database
        self._read_stored_quotes()

        # compare the dates between downloaded and saved dates
        s1 = self.quote_downloaded['date']
        s2 = self._quote_saved['date']
        newdates = pd.Series(np.setdiff1d(s1.values, s2.values))
        newdates = newdates.values

        # extract the lines to save
        quote_to_save = self.quote_downloaded[self.quote_downloaded['date'].isin(newdates)]

        self.log_message("Found {0} quotes to save".format(len(quote_to_save)))

        self.quote_to_save = quote_to_save

        self.debug_message("Currency of quote_to_save: {0}".format(self.quote_to_save['currency'].unique()))
        self.debug_message("Currency of quote_saved  : {0}".format(self._quote_saved['currency'].unique()))

        # check if the quote to save and saved quote have the same currency
        if len(self.quote_to_save) != 0 and len(self._quote_saved) != 0:
            self.debug_message("Comparing currencies of saved and downloaded quote")
            if self._quote_saved['currency'].unique() != self.quote_to_save['currency'].unique():
                self.log_message("Cannot save new quote, wrong currency compared to saved quote")
                self.quote_to_save = None
            else:
                self.debug_message("Saved quote and quote to save have the same currency: {0}".format(
                    self.quote_to_save['currency'].unique()))
        elif len(self.quote_to_save) == 0 and len(self._quote_saved) != 0:
            self.log_message("Nothing to save")
        elif len(self.quote_to_save) != 0 and len(self._quote_saved) == 0:
            self.log_message("No quote saved, adding new entry")

    def _prepare_raw_quote_for_saving(self, raw_quote):
        '''Prepare the quote dataframe for saving'''
        # find columns which are not in the dataframe and replace them with zeros
        for _col in ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']:
            if _col not in raw_quote.columns:
                raw_quote = raw_quote.assign(**{_col: pd.Series(np.zeros(len(raw_quote))).values})

        self.quote_downloaded = raw_quote

        if self.quote_currency == 'GBP':
            self.log_message("Currency of downloaded quote is GBp, transforming to GBP")
            for _k in ['Open', 'High', 'Low', 'Close']:
                self.quote_downloaded[_k] = self.quote_downloaded[_k] / 100.

        # add columns with the name and isin of the stock
        self.quote_downloaded = self.quote_downloaded.assign(
            name=pd.Series([self.name for _ in range(len(self.quote_downloaded))]))
        self.quote_downloaded = self.quote_downloaded.assign(
            isin=pd.Series([self.isin for _ in range(len(self.quote_downloaded))]))
        self.quote_downloaded = self.quote_downloaded.assign(
            currency=pd.Series([self.quote_currency for _ in range(len(self.quote_downloaded))]))
        self.quote_downloaded = self.quote_downloaded.assign(
            exchange=pd.Series([self.quote_exchange for _ in range(len(self.quote_downloaded))]))

        # re-order the elements
        self.quote_downloaded = self.quote_downloaded[['Date', 'name', 'isin', 'exchange',
                                                       'currency', 'Open', 'High', 'Low', 'Close', 'Volume']]

        # re-name the elements
        self.quote_downloaded.columns = ['date', 'name', 'isin', 'exchange', 'currency', 'open',
                                         'high', 'low', 'close', 'volume']

        # put date in the correct format
        self.quote_downloaded['date'] = self.quote_downloaded['date'].apply(get_datetime)

        # Remove NaN entries
        self.quote_downloaded = self.quote_downloaded.dropna(thresh=6)

        # produce the saveable object
        self._extract_unsaved_rows()

    def _download_quote_yahoo(self, useexchange='old', force_exchange=None, attempts=6):
        '''Download the latest stock quote from yahoo finance'''
        ISIN, ticker = self.isin, self.ticker
        # self.log_message("Donwloading quote with ticker symbol {0}".format(self.ticker))
        # if self.branch is not None:
        #     self.log_message("Found different ticker symbol for yahoo finance")
        #     self.log_message("Morningstar: {0}, Yahoo: {1}".format(self.ticker, self.branch))
        #     ticker = self.branch

        verbose = self.verbose
        key = ISIN[0:2]

        _currency = self.keyratios['currency'].unique()[0]
        # self.log_message("Found the following unit of the key ratios: {0}".format(_currency))

        exchange = {}
        exchange['EUR'] = ['.BR', '.VI', '.DE', '.F', '.SG', '.BE', '.DU', '.HM', '.HA', '.MU', '.PA', '.MC']
        exchange['USD'] = ['']
        exchange['CHF'] = ['.VX', '.SW']
        exchange['GBP'] = ['.L', '.IL']

        if (useexchange == 'old' and len(self._quote_saved) > 0):
            exchange = {}
            ex = self._quote_saved['exchange'].values[0].split()[-1].split('.')[-1]
            cur = self._quote_saved['currency'].values[0]
            if '.' in self._quote_saved['exchange'].values[0].split()[-1]:
                exchange[cur] = [".{0}".format(ex)]
            else:
                exchange[cur] = ['']

        if force_exchange is not None:
            exchange = {}
            exchange[_currency] = [force_exchange]

        # assign the start and end date
        start = dt.datetime(1900, 1, 1)
        end = dt.datetime.today()

        if useexchange != 'old' and _currency not in exchange.keys():
            if self.verbose:
                print("Currency not supported: {0}".format(_currency))
            return
        # prepare and perform the query
        quotes = {}
        load_successful = False
        for ex in exchange[_currency]:  # loop over all exchanges for the given currency
            print(ex)
            for i in range(1, attempts):  # try multiple times
                try:
                    print(ex)
                    quotes[ex] = web.get_data_yahoo("{0}{1}".format(ticker, ex), start, end)
                    # web.DataReader("{0}{1}".format(ticker,ex), 'yahoo', start, end)  # query
                    load_successful = True
                    # some output message when successful
                    if verbose:
                        print('succes')
                        # self.log_message('Successfully loaded quote for {0} from yahoo, exchange {1}'.format(ticker,ex))
                    break
                except:
                    if verbose:
                        print('fail')
                        # self.log_message('Attempts to load {0} quote: {1}/5'.format("{0}{1}".format(ticker,ex),i))
                    continue

        if not load_successful:
            if verbose:
                self.log_message("No quote found.")
            return

        # find the longest dataframe and prepare it for saving
        self.quotes_yahoo = quotes
        print(quotes)
        longest_quote, longest_key = self._yahoo_get_longest_quote(self.quotes_yahoo)
        print(longest_key)

        # move date from index to column
        if 'Date' not in longest_quote.index:
            print(quotes)
            print(longest_quote)
            longest_quote.reset_index(level=0, inplace=True)

        longest_quote = longest_quote[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

        longest_quote = self._find_splits(longest_quote)

        self.log_message(
            'Check stock info at https://finance.yahoo.com/quote/{0}{1}?p={0}{1}'.format(ticker, longest_key))

        self.quote_yahoo_raw = longest_quote  # raw quote
        self.quote_currency = _currency  # currency of the quote
        self.quote_exchange = "Y {0}{1}".format(ticker, longest_key)
        self._prepare_raw_quote_for_saving(self.quote_yahoo_raw)
        self._save_in_sql()
