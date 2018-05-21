from QAntTom import get_datetime
from QAntTom import convert_sql_date_to_datetime_date
from QAntTom import pd
from QAntTom import sqlite3
from QAntTom import databasedir, databasequotes
from QAntTom import dt
from QAntTom import np
from QAntTom import get_datetime

from pandas.tseries.offsets import Week, MonthOffset, YearOffset
import pandas_datareader.data as web

import logging


class Quotes:
    """
    Class that contains data about quotes
    """

    def __init__(self, isin, name, exchange, ticker):
        self.isin = isin
        self.name = name
        self.exchange = exchange
        self.ticker = ticker

        # init
        self.quote = pd.DataFrame()
        self.quote_downloaded = pd.DataFrame()
        self._quote_saved = pd.DataFrame()
        self.quote_to_save = pd.DataFrame()
        self.quote_currency = None

        self.log = logging.getLogger('quantlog')

        # init using methods
        self._read_stored_quotes()
        self.lastquote, self.quote_cur = self.get_lastprice()
        self.volatility_day, self.volatility_year = self.calculate_volatility()

    def _read_stored_quotes(self):
        """
        Load the quotes for the current stock from the database
        """
        self.log.debug("Reading saved quote")

        cnx = sqlite3.connect(databasequotes)
        quote_saved = pd.read_sql_query("SELECT * FROM quotes WHERE ISIN = '{0}';".format(self.isin), cnx)
        cnx.close()

        self.log.info("Found {0} quotes".format(len(quote_saved)))

        # convert date strings to datetime objects
        quote_saved['date'] = pd.to_datetime(quote_saved.date).dt.date

        self._quote_saved = quote_saved
        self.quote = quote_saved

    def get_lastprice(self):
        """
        Getting the last close price and the currency in which it is returned
        :return: last close value, currency
        """
        self.log.info('Getting last price for {}'.format(self.isin))
        return self.quote[self.quote.date == self.quote.date.max()].close.values, self.quote.currency.values[0]

    def get_earlier_price(self, daysago):
        """
        Get the prices (daysago) days and the currency
        :param daysago: number of days to go back from last value
        :return: last close value daysago back, currency
        """
        self.log.info('Getting earlier price for {} - {} days ago'.format(self.isin, daysago))
        return self.quote[self.quote.date == (self.quote.date.max() - dt.timedelta(days=daysago))].close.values, \
               self.quote.currency.values[0]

    def find_splits(self):
        """
        Find historic splits from quote
        :param quote: stock quote
        :return:
        """
        self.log.debug('Looking for splits for quote {}'.format(self.isin))
        relchange = self.quote['close'].diff() / self.quote['close']
        splits = (relchange[relchange < -0.5] * (-1.) + 1.).round()

        self.log.info('Found {} splits for quote {}'.format(len(splits), self.isin))
        if len(splits) > 20:
            self.log.warning("Found suspiciously many potential splits ({0}), escaping".format(len(splits)))
            return self.quote

        for i, splitdate in enumerate(self.quote.loc[splits.index, 'date']):
            # print("Splitdate: {0}".format(splitdate))
            self.quote.loc[self.quote.Date <
                           splitdate, 'close'] = self.quote.loc[self.quote.Date <
                                                                splitdate, 'close'] / np.array(splits)[i]
        return self.quote

    def calculate_volatility(self):
        """
        Calculates volatilies over:
            - day
            - year
        :return: volatility day, year
        """
        self.log.debug('Calculating volatilities...')
        self.dailychange = self.quote['close'].diff().shift(-1) / self.quote['close'][:-1]
        volatility_day = self.dailychange.std()
        volatility_year = volatility_day * np.sqrt(252)
        self.log.debug('Daily volatility {} and yearly {}'.format(volatility_day, volatility_year))
        return volatility_day, volatility_year

    def _yahoo_get_longest_quote(self, quotes):
        """
        Return the longest quote downloaded from the different exchanges on yahoo.
        :param quotes:
        :return:
        """

        self.log.debug('Getting longest quote downloaded from yahoo')
        longest_quote = max(quotes, key=lambda k: len(quotes[k]))

        return quotes[longest_quote], longest_quote

    def save_in_sql(self):
        """
        Save the current quote to the database
        """

        if self.quote_to_save.empty():
            self.log.warning('Could not find quote {} to save'.format(self.isin))
            return

        self.log.debug('Saving quote {} to database.'.format(self.isin))

        cnx = sqlite3.connect(databasequotes)
        self.quote_to_save.to_sql('quotes', cnx, if_exists='append', index=False)
        cnx.close()

        self.log.info("Successfully saved {0} entries in quote database".format(len(self.quote_to_save)))

    def _extract_unsaved_rows(self):
        """
        Extract the unsaved rows
        """

        self.log.debug('Extracting unsaved rows.')

        # get the quotes stored in the database
        self._read_stored_quotes()

        # compare the dates between downloaded and saved dates
        s1 = self.quote_downloaded['date']
        s2 = self._quote_saved['date']
        newdates = pd.Series(np.setdiff1d(s1.values, s2.values))
        newdates = newdates.values

        # extract the lines to save
        quote_to_save = self.quote_downloaded[self.quote_downloaded['date'].isin(newdates)]

        self.log.info("Found {0} quotes to save".format(len(quote_to_save)))

        self.quote_to_save = quote_to_save

        self.log.info("Currency of quote_to_save: {0}".format(self.quote_to_save['currency'].unique()))
        self.log.info("Currency of quote_saved  : {0}".format(self._quote_saved['currency'].unique()))

        # check if the quote to save and saved quote have the same currency
        if len(self.quote_to_save) != 0 and len(self._quote_saved) != 0:
            self.log.info("Comparing currencies of saved and downloaded quote")
            if self._quote_saved['currency'].unique() != self.quote_to_save['currency'].unique():
                self.log.info("Cannot save new quote, wrong currency compared to saved quote")
                self.quote_to_save = None
            else:
                self.log.info("Saved quote and quote to save have the same currency: {0}".format(
                    self.quote_to_save['currency'].unique()))
        elif len(self.quote_to_save) == 0 and len(self._quote_saved) != 0:
            self.log.info("Nothing to save")
        elif len(self.quote_to_save) != 0 and len(self._quote_saved) == 0:
            self.log.info("No quote saved, adding new entry")

    def _prepare_raw_quote_for_saving(self, raw_quote ):
        """
        Prepare the quote frame for saving to sql database
        :param raw_quote: raw frame

        """

        self.log.debug('Prepare the quote dataframe for saving...')

        # find columns which are not in the frame and replace them with zeros
        for _col in ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']:
            if _col not in raw_quote.columns:
                raw_quote = raw_quote.assign(**{_col: pd.Series(np.zeros(len(raw_quote))).values})

        self.quote_downloaded = raw_quote

        if self.quote_currency == 'GBP':
            self.log.debug("Currency of downloaded quote is GBp, transforming to GBP")
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
        # TODO : clean up and add methods from morningstar
        """
        Download the latest stock quote from yahoo finance
        :param useexchange:
        :param force_exchange:
        :param attempts:
        :return:
        """

        ISIN, ticker = self.isin, self.ticker
        # self.log_message("Downloading quote with ticker symbol {0}".format(self.ticker))
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