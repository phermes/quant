import datetime as dt
import sqlite3

import numpy as np
import pandas as pd


class fundamentals:
    def __init__(self):
        pass

    def _get_keyratios(self):
        # load the entries for the current stock from the database
        cnx = sqlite3.connect('database/stocks_keyratios.db')
        sql = pd.read_sql_query("SELECT * FROM fundamentals WHERE ISIN = '{0}';".format(self.isin), cnx)
        cnx.close()
        sql = sql.sort_values(by='year', ascending=False)
        sql = sql.reset_index(drop=True)

        self.keyratios = sql

    def _get_pe_for_year(self, pe_year, month=4, day=1, detailed=False):
        '''Get the price/earnings ratio for every day of the subsequent year'''
        # get the min/max date for selecting the quotes
        mindate = dt.date(pe_year + 1, month, day)
        maxdate = dt.date(pe_year + 2, month, day)

        # get the eps for the year under consideration
        eps = self.keyratios[self.keyratios.year == pe_year]['EarningsPerShare'].values[0]

        if eps is None:
            return None

        # extract the quote 
        quote = self.quote[(self.quote.date > mindate) & (self.quote.date < maxdate)]

        quote = quote.assign(pe=(quote.close / eps).values)
        quote = quote.assign(eps=np.ones(len(quote)) * eps)

        if detailed:
            return quote
        return quote[['date', 'close', 'pe', 'eps']]

    def _get_per_cdf(self):
        '''Calculate the values of the cumulative distribution function (CDF) of the PER distribution'''

        # check if per_table is none and abort if so. redundant because _get_all_pe should not call this function.
        if self.per_table is None:
            self.per_cdf = None
            return

        # calculate the CDF table
        _data = self.per_table['pe']
        per_cdf = []
        for _q in np.arange(0, 1, 0.01):
            per_cdf.append([_data.quantile(_q), _q])
        per_cdf = pd.DataFrame(per_cdf, columns=['per', 'quantile'])
        self.per_cdf = per_cdf

    def _get_all_pe(self, detailed=False):
        '''Creates an object with the price to earnings ratio for every day of all years'''

        # check if the currencies are the same; otherwise abort
        _quotecur = self.quote['currency'].unique()
        _keyratiocur = self.keyratios['currency'].unique()
        if _quotecur != _keyratiocur:
            self.error_message('Quote and keyratio table not of the same currency')
            self.error_message('Cant populate per_table and per_cdf')
            self.per_table = None
            self.per_cdf = None
            return

            # initialize the final dataframe
        finalquote = None

        # loop over the last 10 years
        for year in self.keyratios['year'][0:10]:
            if finalquote is None:
                finalquote = self._get_pe_for_year(year, detailed=detailed)
            else:
                finalquote = finalquote.append(self._get_pe_for_year(year, detailed=detailed), ignore_index=True)
            year += 1

        # finalize the dataframe
        self.per_table = finalquote
        self.per_table = self.per_table.sort_values('date', ascending=False)  # sort the table
        self.per_table = self.per_table.reset_index(drop=True)  # reset the index

        # get the present PER
        self.per = self.per_table.iloc[0]['pe']

        # calculate the CDF
        self._get_per_cdf()
