from QAntTom import get_datetime
from QAntTom import convert_sql_date_to_datetime_date
from QAntTom import pd
from QAntTom import sqlite3
from QAntTom import databasedir, databasequotes
from QAntTom import dt
from QAntTom import np

import logging


class Quotes:
    """
    Class that contains data about quotes
    """

    def __init__(self, isin):
        self.isin = isin
        self.quote = pd.DataFrame()
        self._quote_saved = pd.DataFrame()

        self.log = logging.getLogger('quantlog')

        # init using methods
        self._read_stored_quotes()
        self.lastquote, self.quote_cur = self.get_lastprice()

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
        Find hirostic splits from quote
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
