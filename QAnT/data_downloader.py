import datetime
import datetime as dt
import os
import sqlite3
import urllib.request

import numpy as np
import pandas as pd

from QAnT.output import logging
from QAnT import stock

# TOM was still loaded from stocks/quotes
from QAnT.quotes import convert_sql_date_to_datetime_date

# TOM : general location databases
databasedir = '/Users/tommertens/PycharmProjects/quant/'


# TOM : added logging to avoid unresolved references to debug and logging messages
class keyratios(logging):
    def __init__(self):
        # TOM variables that are not initialized
        # init from super class - not so clear where everything comes from - difficult to debug
        super().__init__()

    def _download_keyratios_to_file(self):
        """
        Method to download the keyratios to a temporary directory
        """

        # TOM : not used
        # cnx = sqlite3.connect(databasedir + 'database/stockDB.db')

        # TOM : not used
        # currencies = pd.read_sql("SELECT * FROM currencies", cnx)

        # TOM: define as literal
        country = {'AU': 'AUS'}

        for c in ['DE', 'BE', 'ES', 'NL', 'IE', 'GR', 'US', 'FR', 'CA', 'TH', 'IL', 'LU']:
            country[c] = 'DEU'

        country['CH'] = 'CH'
        country['GB'] = 'GB'
        country['JP'] = 'JP'
        country['IT'] = 'IT'

        # TOM : not used
        # weblink = 'http://financials.morningstar.com/ajax/exportKR2CSV.html?t=FB'

        ticker = self._ticker_ms
        region = country[self.isin[0:2]]
        prefix = self.isin[0:2]

        if not os.path.exists('tmpfiles/'):
            os.makedirs('tmpfiles/')

        outfile = 'tmpfiles/{0}_{1}.csv'.format(prefix, ticker)

        self.filename = outfile

        if os.path.isfile(outfile):
            print('File existing: {0}'.format(outfile))
            return

        saved = self.keyratios
        self.log_message("Found {0} keyratios".format(len(saved)))

        basestr = 'http://financials.morningstar.com/ajax/exportKR2CSV.html?'
        weblink = '{0}t={1}&region={2}&culture=en_US'.format(basestr, ticker, region)

        with urllib.request.urlopen(weblink) as response:
            html = response.read()

        if len(html) > 0:
            self.debug_message('Writing output to {0}'.format(outfile))

        self.log_message('Name: {0:20s}   |   {1:20s}'.format(self.name, str(html).split('\\n')[0].split("for")[-1]))

        if self._control:
            _ = input("Press Enter to continue...")

        with open(outfile, 'wb') as f:
            f.write(html)
            # return html

    def load_keyratios(self, deletecsv=False):
        """
        Method to load keyratios from the temporary directory/files to database
        :param deletecsv: option to delete the temporary csv file
        :return:
        """

        self._download_keyratios_to_file()

        filename = self.filename
        try:
            self.downloaded_keyratios = pd.read_csv(filename, skiprows=2)
        except IOError:
            # TOM : generate error log message if file not found
            self.log_message('File {} not found - no keyratios added'.format(self.filename))
            self.downloaded_keyratios = None
            return

        # TOM : correct the column names of the loaded keyratios table
        self._correct_colnames()
        self.downloaded_keyratios = self._format_keyratios()
        self.create_table_for_sql()
        self._save_keyratios()

        if deletecsv:
            ticker = self._ticker_ms
            prefix = self.isin[0:2]

            outfile = 'tmpfiles/{0}_{1}.csv'.format(prefix, ticker)
            os.remove(outfile)

            self.log_message("Removing temporary output file {0}".format(outfile))

    def _save_keyratios(self):
        """
        Saves the downloaded keyratios which are not yet in the sql database
        :return:
        """

        today = dt.date.today()

        downl = self.downloaded_keyratios
        saved = self.keyratios

        self.log_message(
            "Found saved and downloaded keyratio tables with sizes {0} and {1}".format(len(downl), len(saved)))

        yrsaved = saved['year'].values
        yrdownl = downl['year'].values
        years_to_save = np.setdiff1d(yrdownl, yrsaved)
        years_to_save = years_to_save[years_to_save != today.year]

        try:
            df_to_save = downl[downl['year'] == years_to_save[0]]
        except IndexError:
            self.log_message("No entries to save")
            return

        for y in years_to_save[1:]:
            df_to_save = df_to_save.append(downl[downl['year'] == y])

        df_to_save = df_to_save.reset_index(drop=True)
        self._keyratios_to_save = df_to_save

        self.log_message("Saving {0} entries in fundamentals database".format(len(df_to_save)))

        cnx = sqlite3.connect(databasedir + 'database/stocks_keyratios.db')

        if df_to_save is not None:
            df_to_save.to_sql('fundamentals', cnx, if_exists='append', index=False)

        cnx.close()

    def _correct_colnames(self):
        """
        Method for fixing keyratios column names
        :return:
        """

        self.debug_message("Correcting column names")

        newcols = []
        for i in self.downloaded_keyratios.columns:
            if i == 'TTM':
                newcols.append(newcols[-1] + 1)
                continue
            if 'Unnamed' in i:
                newcols.append('year')
                continue
            newcols.append(int(i.split('-')[0]))

        self.downloaded_keyratios.columns = newcols

    def _format_keyratios(self):
        """
        Formatting keyratios table to put in database
        :return:
        """

        self.debug_message("Formatting keyratios")
        cnx = sqlite3.connect(databasedir + 'database/stockDB.db')
        currencies = pd.read_sql("SELECT * FROM currencies", cnx)
        currencies = currencies.values.flatten()

        pdf = self.downloaded_keyratios
        pdf = pdf.transpose()

        cols_new = []
        cols_to_use = []

        cols_old = np.array(pdf[pdf.index == 'year'])[0]
        currency = self._find_currency(cols_old)

        self.debug_message("Cleaning up the column strings")
        for string in cols_old:
            try:
                newstring = string
                newstring = newstring.replace(' ', '')
                newstring = newstring.replace('%', '')
                newstring = newstring.replace('Mil', '')
                newstring = newstring.replace('*', '')
                newstring = newstring.replace('(', '')
                newstring = newstring.replace(')', '')
                newstring = newstring.replace("'", '')
                for j in currencies:
                    newstring = newstring.replace(j, '')
            except AttributeError:
                newstring = 'NaN'

            if newstring in cols_new:
                newstring = '{0}_{1}'.format(newstring, len(cols_new) + 1)
            cols_new.append(newstring)

        try:
            pdf.columns = cols_new
        except ValueError:
            return cols_old, cols_new
        pdf = pdf[1:]

        for c in cols_new:
            if '_' not in c and '&' not in c:
                if c not in ['NaN', '3-YearAverage', 'Efficiency', 'MarginsofSales', 'Growth',
                             'BalanceSheetItemsin', 'Liquidity/FinancialHealth', 'Profitability',
                             'CashFlowRatios']:
                    cols_to_use.append(c)

        pdf = pdf[cols_to_use]
        pdf = pdf.replace(to_replace=",", value="", regex=True)

        self.debug_message("Formatting keyratios")
        self.downloaded_keyratios = pdf

        pdf = pdf.astype('float')
        pdf = pdf[[
            'Shares',
            'Revenue',
            'NetIncome',
            'EBTMargin',
            'EarningsPerShare',
            'Dividends',
            "TotalStockholdersEquity",
            'ReturnonEquity',
            'BookValuePerShare',
            'OperatingCashFlow',
            'GrossMargin',
            'OperatingIncome',
            'OperatingMargin',
            'CapSpending',
            'FreeCashFlow',
            'FreeCashFlowPerShare',
            'WorkingCapital',
            'COGS',
            'TaxRate',
            'NetMargin',
            'AssetTurnoverAverage',
            'ReturnonAssets',
            'FinancialLeverageAverage',
            'ReturnonInvestedCapital',
            'InterestCoverage'
        ]]

        self.currency = currency
        return pdf

    # TOM : static method, on dependence on self
    @staticmethod
    def _find_currency(cols_old):
        """
        Method to find currencies associated to the keyratios
        :param cols_old:
        :return:
        """
        cnx = sqlite3.connect(databasedir + 'database/stockDB.db')
        currencies = pd.read_sql("SELECT * FROM currencies", cnx)
        currencies = currencies.values.flatten()

        for cur in currencies:
            if 'Earnings Per Share {0}'.format(cur) in cols_old:
                #                 print 'ISIN {0} found currency: {1}'.format(self.isin,cur)
                return cur

    def create_table_for_sql(self):
        """
        Create a frame to add to sql database
        :return:
        """

        df = self.downloaded_keyratios
        cur = self.currency

        isin, name, ticker = self.isin, self.name, self.ticker

        df.reset_index(inplace=True)
        df = df.rename(columns={'index': 'year'})

        # prepare the columns containing isin, name and currency
        isin_array = []
        curr = []
        nama = []
        for i in range(len(df)):
            isin_array.append(isin)
            curr.append(cur)
            nama.append(name)

        df = df.assign(name=nama)
        df = df.assign(ISIN=isin_array)
        df = df.assign(currency=curr)

        # change the shape of the columns
        cols = list(df.columns)
        for i in range(0, 3):
            cols = cols[-1:] + cols[:-1]

        df = df[cols]
        self.downloaded_keyratios = df

        # return df


# TOM : added logging and stock to solve unresolved references
class quarterly_report(logging, stock):
    # TOM : you need to call the super class init for initializing the necessary class attributes
    def __init__(self):
        super().__init__()

    def _find_quarterly_report_dates_table(self, string):
        """
        Returns the table containing dates for the quarterly report
        :param string:
        :return:
        """
        tables = pd.read_html('http://www.finanzen.ch/termine/{0}'.format(string))
        self.log_message("Downloading quarterly report dates from")
        self.log_message("http://www.finanzen.net/termine/{0}".format(string))

        for table in tables:
            try:
                if 'Quartalszahlen' in table[0].values:
                    self.debug_message("Table before cleaning")
                    self.debug_message(table)
                    table = table[table[0] == 'Quartalszahlen']
                    self.log_message("Found quarterly report table with {0} entries".format(len(table)))
                    return table
            except KeyError:
                continue
        return None

    def _read_stored_quarterly_report_dates(self):
        """
        Load the stored quarterly report date for the current stock from the database.
        :return:
        """

        self.log_message("Reading saved quarterly report dates")
        cnx = sqlite3.connect(databasedir + 'database/stocks_dates.db')
        dates_saved = pd.read_sql_query("SELECT * FROM quarterly_report WHERE isin = '{0}';".format(self.isin), cnx)
        cnx.close()

        # convert date strings to datetime objects
        dates_saved['report_date'] = dates_saved['report_date'].apply(convert_sql_date_to_datetime_date)

        self._quarterly_report_dates_saved = dates_saved
        self.quarterly_report_dates = dates_saved

    def _save_unsaved_quarterly_report_dates(self):
        """
        Find entries in the downloaded dataset which are not in the saved dataset
        :return:
        """

        # download the quarterly report dates from the web
        self._get_quarterly_report_dates()

        # read the stored quarterly report dates stored in the database
        self._read_stored_quarterly_report_dates()

        if self._quarterly_report_dates_downloaded is None:
            self.log_message("Could not download quarterly report dates")
            return

        # compare the dates between downloaded and saved dates
        s1 = self._quarterly_report_dates_downloaded['report_date']
        s2 = self._quarterly_report_dates_saved['report_date']
        newdates = pd.Series(np.setdiff1d(s1.values, s2.values))
        newdates = newdates.values

        # extract the lines to save =  dates in newdates
        quarterly_report_dates_to_save = self._quarterly_report_dates_downloaded[
            self._quarterly_report_dates_downloaded['report_date'].isin(newdates)]

        self.log_message("Found {0} quarterly report dates to save".format(len(quarterly_report_dates_to_save)))
        # self._quarterly_report_dates_to_save = quarterly_report_dates_to_save

        # save the lines in the sqlite database
        if quarterly_report_dates_to_save is None:
            self.log_message("No quarterly report dates to save")
            return

        self.debug_message("Saving to database/stocks_dates.db")
        cnx = sqlite3.connect(databasedir + 'database/stocks_dates.db')
        quarterly_report_dates_to_save.to_sql('quarterly_report', cnx, if_exists='append', index=False)
        cnx.close()

        self.log_message(
            "Successfully saved {0} entries in quarterly report database".format(len(quarterly_report_dates_to_save)))

        # update self.quarterly_report_dates
        self._read_stored_quarterly_report_dates()

    def _get_quarterly_report_dates(self):
        """
        Returns a dataframe containing the last quarterly reports
        :return:
        """
        table = self._find_quarterly_report_dates_table(self._fn_link)

        # return if table is empty
        if table is None:
            self._quarterly_report_dates_downloaded = None
            return

            # transform the date strings into datetime objects
        output = []
        for column in table.columns:
            for element in table[column]:
                try:
                    output.append(datetime.datetime.strptime(element, "%d.%m.%y").date())
                except:
                    pass
        output = [(self.name, self.isin, d) for d in output]
        output = pd.DataFrame(output, columns=['name', 'isin', 'report_date'])

        self._quarterly_report_dates_downloaded = output
