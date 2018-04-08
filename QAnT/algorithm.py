import datetime as dt
import sqlite3

import numpy as np
import pandas as pd

from QAnT import stock


# TOM : optimized imports

class algo(stock):
    """
    Class providing asset analysis tools
    """

    def __init__(self):
        # TOM: not initialized

        self.six_month_return = None
        self.twelve_month_return = None
        self.mean_earnings_growth = None
        self.summary = None
        self.fairprice_low = None
        self.fairprice_high = None
        self.fairprice = None
        self.price = None
        self.fairprice_pe = None

    def _initialize_algo(self):
        """
        Initialize the returned dataframe when algorithm is run
        """

        cols = ['Name', 'ISIN', 'Parameter', 'Value', 'Point']
        self.quant_result = pd.DataFrame(columns=cols)

    def _add_result(self, para, val, point):
        """
        Add the result of the individual analysis to the total result.
        :param para: parameters
        :param val: values
        :param point: resulting points
        """

        cols = ['Name', 'ISIN', 'Parameter', 'Value', 'Point']

        # TOM: confusing as self.name and self.isin are not defined in this class
        # my IDE returns 'unresolved attribute reference' and might cause difficult to trace bugs
        res = [[self.name, self.isin, para, val, point]]

        self.quant_result = self.quant_result.append(pd.DataFrame(res, columns=cols), ignore_index=True)

    def _get_best_column(self, *args):
        """
        If multiple columns can be used for the analysis, select the column with the best data quality
        """

        vals = args
        _length = []

        for _i, _s in enumerate(vals):
            _length.append([_i, len(self.keyratios[self.keyratios[_s].isnull()])])

        _length = np.array(_length)
        _bestcol = vals[_length[_length[:, 1].argmin()][0]]

        return _bestcol

    def positive_earnings(self):
        # TOM : removed verbose - not used
        """
        Check if the earnings history includes years of negative earnings
        """

        _col = self._get_best_column('NetIncome', 'EarningsPerShare')

        if (self.keyratios[_col] < 0).any():
            # TOM : remove val - unused
            # val = -1
            point = -1
        else:
            # val = 1
            point = 1
        self._add_result('OnlyPositiveEarnings', 0, point)

    def six_month_price_change(self):
        """
        Get change over six months
        """
        # TOM : alternative algo for finding closest to six month date
        # >>> df['Datum'] = pd.to_datetime(df.Datum, format='%d.%m.%y')
        # >>> date = dt.datetime.now().date()
        # >>> print (date)
        # >>> 2016-07-17
        #
        # >>> diff = (df.Datum - date)
        # >>> print (diff)
        # >>> 0    -25 days
        # >>> 1   -129 days
        # >>> 2   -263 days
        # >>> 3   -354 days
        # >>> 4   -439 days
        # >>> 5   -445 days
        # >>> 6   -493 days
        # >>> 7      2 days
        # >>> Name: Datum, dtype: timedelta64[ns]
        #
        # >>> indexmax = (diff[(diff < pd.to_timedelta(0))].idxmax())
        #
        # >>> print (df.ix[[indexmax]])

        _dates = self.quote['date']
        latest_date = self.quote[_dates == _dates.max()]
        latest_date = latest_date['date'].values[0]
        latest_quote = self.quote[self.quote['date'] == latest_date]['close'].values[0]
        six_months_ago = [latest_date - dt.timedelta(days=d) for d in range(180, 190)]

        # find the quote closest to six months ago
        for d in six_months_ago:
            try:
                quote_six_months_ago = self.quote[self.quote['date'] == d]['close'].values[0]
                break
            except IndexError:
                continue

        # TOM: tricky as in principle quote_six_months_ago can be referenced without being defined !!!
        # calculate the return
        six_month_return = latest_quote / quote_six_months_ago
        six_month_return = six_month_return - 1.
        six_month_return = six_month_return * 100.

        # write out the result
        if six_month_return > 5.:
            point = 1
        elif six_month_return < -5.:
            point = -1
        else:
            point = 0

        self.six_month_return = six_month_return

        self._add_result('SixMonthReturn', '{0:0.1f}'.format(six_month_return), point)

    def twelve_month_price_change(self):
        """
        Get change over twelve months
        """

        _dates = self.quote['date']
        latest_date = self.quote[_dates == _dates.max()]
        latest_date = latest_date['date'].values[0]
        latest_quote = self.quote[self.quote['date'] == latest_date]['close'].values[0]
        twelve_months_ago = [latest_date - dt.timedelta(days=d) for d in range(360, 370)]

        # find the quote closest to six months ago
        for d in twelve_months_ago:
            try:
                quote_twelve_months_ago = self.quote[self.quote['date'] == d]['close'].values[0]
                break
            except IndexError:
                continue

        # TOM: tricky as quote_twelve_months_ago can be referenced without being assigned
        # calculate the return
        twelve_month_return = latest_quote / quote_twelve_months_ago
        twelve_month_return = twelve_month_return - 1.  # TOM : added . otherwise if else expects comparison with int
        twelve_month_return = twelve_month_return * 100.

        # write out the result
        if twelve_month_return > 5.:
            point = 1

        elif twelve_month_return < -5.:
            point = -1

        else:
            point = 0

        self.twelve_month_return = twelve_month_return
        self._add_result('TwelveMonthReturn', '{0:0.1f}'.format(twelve_month_return), point)

    def historic_roe(self):
        """
        Checks if the historic ROE was never below 20%
        """

        _roe = self.keyratios['ReturnonEquity'][1:]
        _roe = _roe[_roe > 0]

        self.log_message("HistoricROE found: {0}".format(_roe))

        if not (_roe < 20).any():
            point = 1
        elif not (_roe < 10).any():
            point = 0
        else:
            point = -1

        _value = _roe.min()
        self._add_result('HistoricROE', '{0:0.1f}'.format(_value), point)

    def present_roe(self):
        """
        Check if ROE is above 20%
        """
        _roe = self.keyratios['ReturnonEquity']
        _val = _roe[0]

        if _val is None:
            self._add_result('PresentROE', '{0:0.1f}'.format(0), -1)
            self.error_message('PresentROE is of type None')
            return

        if not (_val < 20).any():
            point = 1
        elif not (_val < 10).any():
            point = 0
        else:
            point = -1

        self._add_result('PresentROE', '{0:0.1f}'.format(_val), point)

    def present_RoIC(self):
        """
        Check if RoIC is >15%
        """

        _roic = self.keyratios['ReturnonInvestedCapital']
        _val = _roic[0]

        if _val is None:
            self._add_result('PresentRoIC', '{0:0.1f}'.format(0), -1)
            self.error_message('PresentRoIC is of type None')
            return

        if not (_val < 15):
            point = 1
        elif not (_val < 5):
            point = 0
        else:
            point = -1

        self._add_result('PresentRoIC', '{0:0.1f}'.format(_val), point)

    def present_ebtmargin(self):
        """
        Check if EBT margin is above 12%
        """
        _ebtm = self.keyratios['EBTMargin']
        _val = _ebtm[0]

        if _val is None:
            self._add_result('EBTMargin', '{0:0.1f}'.format(0), -1)
            self.error_message('EBTMargin is of type None')
            return

        if _val > 12:
            point = 1
        elif _val < 6:
            point = -1
        else:
            point = 0

        self._add_result('EBTMargin', '{0:0.1f}'.format(_val), point)

    def earnings_growth(self):
        """
        Calculates the mean earnings growth over the last ten years
        """

        _col = self._get_best_column('NetIncome', 'EarningsPerShare')

        mean_earnings_old = self.keyratios[_col][8:11].mean()
        mean_earnings_new = self.keyratios[_col][0:3].mean()

        # check if mean_earnings_old not zero, to prevent ZeroDivisionError
        try:
            _mean_growth_all = (mean_earnings_new - mean_earnings_old) / mean_earnings_old
        except ZeroDivisionError:
            _mean_growth_all = -0.99

        # TOM : this formula I do not completely understand - you will need to explain :)
        # continuous compounding over 8 years 1 / 8 to get annual growth
        _mean_growth_pa = (1 + _mean_growth_all) ** (1. / 8.)
        _mean_growth_pa = _mean_growth_pa - 1

        if type(_mean_growth_pa) is complex:
            _mean_growth_pa = _mean_growth_all / 8.

        if _mean_growth_pa > 0.05:
            point = 1
        elif _mean_growth_pa < 0:
            point = -1
        else:
            point = 0

        self.log_message("Earnings Growth: {0}".format(_mean_growth_pa))

        self.mean_earnings_growth = _mean_growth_pa
        self._add_result('AnnualEarningsGrowth', '{0:0.1f}'.format(_mean_growth_pa * 100), point)

    def dividend_growth(self):
        """
        Check if dividend has been growing over ten years
        """

        _latest_div = self.keyratios['Dividends'][0:3].mean()
        _previous_div = self.keyratios['Dividends'][8:11].mean()
        _div_growth = (_latest_div - _previous_div) / _previous_div

        self.log_message("Checking dividend growth")
        self.log_message("Latest dividend {0}".format(_latest_div))
        self.log_message("Previous dividend {0}".format(_previous_div))

        if _div_growth < 0.25:
            point = -1
        elif _div_growth > 0.80:
            point = +1
        else:
            point = 0

        self._add_result('DividendGrowth', '{0:0.1f}'.format(_div_growth * 100), point)

    def bookvalue_growth(self):
        """
        Check if bookvalue has been growing over ten years
        """
        _latest_bps = self.keyratios['BookValuePerShare'][0:3].mean()
        _previous_bps = self.keyratios['BookValuePerShare'][8:11].mean()
        _bps_growth = (_latest_bps - _previous_bps) / _previous_bps

        if _bps_growth < 0.25:
            point = -1
        elif _bps_growth > 0.80:
            point = +1
        else:
            point = 0

        self._add_result('BookValueGrowth', '{0:0.1f}'.format(_bps_growth * 100), point)

    def equityratio(self):
        """
        Check the equity ratio. So far the limits correspond to non-financial assets.
        """

        equity_ratio = self.keyratios['TotalStockholdersEquity'][0]

        if self.branch == 1:
            self.log_message("Financial company, applying different criteria for equity ratio")
            lowerlimit = 5
            upperlimit = 10
        else:
            lowerlimit = 15
            upperlimit = 25

        if equity_ratio < lowerlimit:
            point = -1
        elif equity_ratio > upperlimit:
            point = 1
        else:
            point = 0

        self._add_result('EquityRatio', '{0:0.1f}'.format(equity_ratio), point)

    def _no_fundamentals(self):
        """
        Check if the fundamentals dataframe is empty
        """

        if len(self.keyratios) == 0:
            self.error_message("No fundamentals found.")
            return True
        return False

    def get_summary(self, save=False, conservative=False):
        """
        Get the algorithm's summary
        :param save: set True to save
        :param conservative:
        :return:
        """
        if self._no_fundamentals():
            return

        self.positive_earnings()
        self.earnings_growth()
        self.bookvalue_growth()
        self.historic_roe()
        self.present_roe()
        self.present_RoIC()
        self.equityratio()
        self.present_ebtmargin()
        self.dividend_growth()
        self.get_fair_price(conservative=conservative)
        self._get_all_pe()
        self.get_fair_price_from_pe()
        # self.six_month_price_change()
        # self.twelve_month_price_change()

        self.summary = pd.DataFrame([[self.name,
                                      self.isin,
                                      self.fairprice,
                                      self.fairprice_pe,
                                      self.price,
                                      self.quant_result['Point'].sum()
                                      ]], columns=['Name', 'ISIN', 'FairPrice', 'FairPricePE', 'Price', 'Points'])

        if save:
            out_conn = sqlite3.connect('/Users/tommertens/PycharmProjects/quant/output/algo_results.db')
            self.log_message('Connected to sqlite database algo_results.db')
            self.log_message('Removing old output')

            c = out_conn.cursor()
            c.execute("DELETE FROM results where ISIN='{0}'".format(self.isin))
            c.execute("DELETE FROM summary where ISIN='{0}'".format(self.isin))
            out_conn.commit()
            # c.close()
            self.log_message('Adding new result')
            self.quant_result.to_sql('results', out_conn, if_exists='append', index=False)
            self.summary.to_sql('summary', out_conn, if_exists='append', index=False)
            out_conn.close()

        self.log_message('Completed get_summary')

    # ratios related to stock price

    def _fair_price(self, holdduration=12, interest=0.02, growth=None):
        """
        Calculate the lower and the upper bound
        :param holdduration:
        :param interest:
        :param growth:
        :return:
        """

        last_eps = self.keyratios['EarningsPerShare'][0:3].mean()
        bookval = self.keyratios['BookValuePerShare'][0]

        if growth is None:
            growth = self.mean_earnings_growth

        if bookval is None:
            bookval = 0

        fairprice = 0

        for y in range(0, holdduration):
            fairprice += (1. + growth) ** y * last_eps / (1. + interest) ** y

        fairprice += bookval

        return fairprice

    def get_fair_price(self, conservative=False):
        """
        calculate fair price interval
        :param conservative:
        :return:
        """
        _low_fair_price = self._fair_price(holdduration=12, interest=0.02, growth=0.0)
        _high_fair_price = self._fair_price(holdduration=12, interest=0.02, growth=None)
        _current_price = self.quote[self.quote.date == self.quote.date.max()]['close'].values[0]

        _fairprice = np.array([_low_fair_price, _high_fair_price]).mean()

        if conservative:
            _fairprice = _low_fair_price

        self.fairprice_low = _low_fair_price
        self.fairprice_high = _high_fair_price
        self.fairprice = _fairprice
        self.price = _current_price

    def get_fair_price_from_pe(self, quantile=0.5, marginofsafety=0.1):
        """
        Get the fair price from the price/earnings ratios
        :param quantile:
        :param marginofsafety:
        :return:
        """
        try:
            self.log_message("Calculating fair price from P/E ratio")
            self.log_message("Selected quantile: {0}".format(quantile))
            _pe_median = self.per_table['pe'].quantile(quantile)
        except TypeError:
            self.error_message('Cant read pe from per_table, fairprice_pe set to 0')
            self.fairprice_pe = 0
            return

        _scaled_median_price = _pe_median * self.keyratios['EarningsPerShare'][0]
        _fair_price = _scaled_median_price * (1 - marginofsafety)

        self.fairprice_pe = _fair_price

    def per_vs_historic_per(self):
        # TOM : removed verbose - not used
        """

        :return:
        """
        self._get_all_pe()

        _current = self.per_table[self.per_table.date == self.per_table.date.max()].pe.values[0]
        _limit1 = self.per_table.pe.quantile(0.33)
        _limit2 = self.per_table.pe.quantile(0.66)

        # TOM : unresolved reference point something missing here
        # temporary solution - needs to be checked by Pascal

        if _current < _limit1:
            point = +1
        elif _current > _limit2:
            point = -1
        else:
            point = 0

        self._add_result('EPSvsHistoricEPS',
                         '{0:0.1f}|{1:0.1f} - {2:0.1f}'.format(_limit1, _limit2, _current), point)
