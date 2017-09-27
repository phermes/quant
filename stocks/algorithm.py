import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt


class algo:
    def _initialize_algo(self):
        self.cols         = ['Name','ISIN', 'Parameter', 'Value', 'Point']
        self.quant_result = pd.DataFrame(columns=self.cols)
    
    def _add_result(self,para,val,point):
        '''Add the result of the individual analysis to the total result.'''
        res = [[self.name, self.isin, para, val, point]]
        self.quant_result = self.quant_result.append(pd.DataFrame(res,columns=self.cols),ignore_index=True)
    
    def get_best_column(self,*args):
        '''If multiple columns can be used for the analysis, select the column with the best data quality'''
        vals = args

        _length = []
        for _i, _s in enumerate(vals):
            _length.append([_i,len(self.keyratios[self.keyratios[_s].isnull()])])
        _length = np.array(_length)
        _bestcol = vals[_length[_length[:,1].argmin()][0]]

        return _bestcol
    
    def positive_earnings(self,verbose=False):
        
        _col = self.get_best_column('NetIncome','EarningsPerShare')
        if (self.keyratios[_col]<0).any():
            val   = -1
            point = -1
        else:
            val   = 1
            point = 1 
        self._add_result('OnlyPositiveEarnings', 0, point)
        
    
    def historic_roe(self):
        '''Checks if the historic ROE was never below 20%'''
        _roe = self.keyratios['ReturnonEquity'][1:]
        _roe = _roe[_roe>0]

        if not (_roe<20).any():
            point = 1
        elif not (_roe<10).any():
            point = 0
        else:
            point = -1
            
        _value = _roe.min()
        self._add_result('HistoricROE', '{0:0.1f}'.format(_value),  point)  
        
        
    def present_roe(self):
        '''Check if ROE is above 20%'''
        _roe = self.keyratios['ReturnonEquity']
        _val = _roe[0]

        if _val is None:
            self._add_result('PresentROE', '{0:0.1f}'.format(0), -1) 
            self.error_message('PresentROE is of type None')            
            return
        
        if not (_val<20).any():
            point = 1
        elif not (_val<10).any():
            point = 0
        else:
            point = -1
            
        self._add_result('PresentROE', '{0:0.1f}'.format(_val), point)  

        
    def present_ebtmargin(self):
        '''Check if EBT margin is above 12%'''
        _ebtm = self.keyratios['EBTMargin']
        _val  = _ebtm[0]
        
        if _val is None:
            self._add_result('EBTMargin', '{0:0.1f}'.format(0),  -1)
            self.error_message('EBTMargin is of type None')
            return

        if _val>12:
            point = 1
        elif _val<6:
            point = -1
        else:
            point = 0
            
        self._add_result('EBTMargin', '{0:0.1f}'.format(_val),  point)  
        
    def earnings_growth(self):
        '''Calculates the mean earnings growth over the last ten years'''
        _col                        =  self.get_best_column('NetIncome','EarningsPerShare')
        
        mean_earnings_old           =  self.keyratios[_col][8:11].mean()
        mean_earnings_new           =  self.keyratios[_col][0:3].mean()
        
        # check if mean_earnings_old not zero, to prevent ZeroDivisionError
        try:
            _mean_growth_all = (mean_earnings_new-mean_earnings_old)/mean_earnings_old
        except ZeroDivisionError:
            _mean_growth_all = -0.99
            
        _mean_growth_pa  = (1+_mean_growth_all)**(1./8.) 
        _mean_growth_pa  = _mean_growth_pa -1
        
        if type(_mean_growth_pa) is complex:
            _mean_growth_pa = _mean_growth_all/8.
        
        if _mean_growth_pa>0.05:
            point = 1
        elif _mean_growth_pa<0:
            point = -1
        else:
            point = 0
            
        self.mean_earnings_growth = _mean_growth_pa
        self._add_result('AnnualEarningsGrowth', '{0:0.1f}'.format(_mean_growth_pa*100), point)  
        
    def dividend_growth(self):
        '''Check if dividend has been growing over ten years'''
        _latest_div   = self.keyratios['Dividends'][0:3].mean()
        _previous_div = self.keyratios['Dividends'][8:11].mean()
        _div_growth = (_latest_div-_previous_div)/(_previous_div)

        if _div_growth<0.25:
            point = -1
        elif _div_growth>1.0:
            point = +1
        else:
            point = 0
        self._add_result('DividendGrowth', '{0:0.1f}'.format(_div_growth*100), point)  
       

    def bookvalue_growth(self):
        '''Check if bookvalue has been growing over ten years'''
        _latest_bps   = self.keyratios['BookValuePerShare'][0:3].mean()
        _previous_bps = self.keyratios['BookValuePerShare'][8:11].mean()
        _bps_growth = (_latest_bps-_previous_bps)/(_previous_bps)

        if _bps_growth<0.25:
            point = -1
        elif _bps_growth>1.0:
            point = +1
        else:
            point = 0
        self._add_result('BookValueGrowth', '{0:0.1f}'.format(_bps_growth*100), point)         
        
    def equityratio(self):
        '''Check the equity ratio. So far the limits correspond to non-financial assets.'''
        equity_ratio = self.keyratios['TotalStockholdersEquity'][0]
        
        if equity_ratio<15.:
            point = -1
        elif equity_ratio>25.:
            point = 1
        else:
            point = 0
        self._add_result('EquityRatio', '{0:0.1f}'.format(equity_ratio), point)   
        
    def _no_fundamentals(self):
        '''Check if the fundmentals dataframe is empty'''
        if len(self.keyratios)==0:
            self.error_message("No fundamentals found.")
            return True
        return False
    
    def get_summary(self,save=False,conservative=False):
        if self._no_fundamentals():
            return
        
        self.positive_earnings()
        self.earnings_growth()
        self.bookvalue_growth()
        self.historic_roe()
        self.present_roe()
        self.equityratio()
        self.present_ebtmargin()
        self.dividend_growth()
        self.get_fair_price(conservative=conservative)
        self._get_all_pe()
        self.get_fair_price_from_pe()

        self.summary = pd.DataFrame([[self.name, 
                                       self.isin,
                                       self.fairprice,
                                       self.fairprice_pe,
                                       self.price,
                                       self.quant_result['Point'].sum()
                                     ]],columns=['Name','ISIN','FairPrice','FairPricePE', 'Price', 'Points'])
        
        if save:

            out_conn        = sqlite3.connect('output/algo_results.db')
            self.log_message('Connected to sqlite database algo_results.db')
            self.log_message('Removing old output')

            c = out_conn.cursor() 
            c.execute("DELETE FROM results where ISIN='{0}'".format(self.isin))
            c.execute("DELETE FROM summary where ISIN='{0}'".format(self.isin))
            out_conn.commit()
            # c.close()
            self.log_message('Adding new result')
            self.quant_result.to_sql('results',out_conn, if_exists='append',index=False)
            self.summary.to_sql('summary',out_conn, if_exists='append',     index=False)
            out_conn.close()


        self.log_message('Completed get_summary')
        
        
# ratios related to stock price

    def _fair_price(self,holdduration=12, interest=0.02, growth = None):
        '''Calculate the lower and the upper bound '''
        
        last_eps = self.keyratios['EarningsPerShare'][0:3].mean()
        bookval  = self.keyratios['BookValuePerShare'][0]
        if growth is None:
            growth        = self.mean_earnings_growth
        
        if bookval is None:
            bookval = 0

        fairprice = 0
        for y in range(0,holdduration):
            fairprice+=(1.+growth)**y*last_eps/(1.+interest)**y
        fairprice +=bookval

        return fairprice

    def get_fair_price(self, conservative=False):
        _low_fair_price  = self._fair_price(holdduration=12,interest=0.02,growth=0.0)
        _high_fair_price = self._fair_price(holdduration=12,interest=0.02,growth=None)    
        _current_price   = self.quote[self.quote.date==self.quote.date.max()]['close'].values[0]
        
        _fairprice       = np.array([_low_fair_price, _high_fair_price]).mean()
        if conservative:
            _fairprice       = _low_fair_price

        self.fairprice_low  = _low_fair_price
        self.fairprice_high = _high_fair_price
        self.fairprice      = _fairprice
        self.price          = _current_price

    def get_fair_price_from_pe(self,quantile=0.5, marginofsafety=0.1):
        try:
            _pe_median   =  self.per_table['pe'].quantile(quantile)
        except TypeError:
            self.error_message('Cant read pe from per_table, fairprice_pe set to 0')
            self.fairprice_pe = 0
            return
            
        _scaled_median_price = _pe_median*self.keyratios['EarningsPerShare'][0]
        _fair_price          = _scaled_median_price*(1-marginofsafety)        
        self.fairprice_pe    = _fair_price
        
    def per_vs_historic_per(self,verbose=False):
        self._get_all_pe()
        
        _current = self.per_table[self.per_table.date == self.per_table.date.max()].pe.values[0]
        _limit1  = self.per_table.pe.quantile(0.33)
        _limit2  = self.per_table.pe.quantile(0.66)
            
        self._add_result('EPSvsHistoricEPS', 
                         '{0:0.1f}|{1:0.1f} - {2:0.1f}'.format(_limit1, _limit2, _current), 
                         point)
