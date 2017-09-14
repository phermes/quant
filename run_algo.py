import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt


# The main script running the algorithm for stock analysis.
# Before running this script, make sure the quotes database is up to date
# 


cnx        = sqlite3.connect('database/stocks_main.db')
xetra      = pd.read_sql("SELECT * FROM XETRA;", cnx)


def get_datetime(inputobj):
    return dt.datetime.date(inputobj)

def convert_sql_date_to_datetime_date(string):
    return dt.datetime.strptime(string,'%Y-%m-%d').date()

class quotes:
    def __init__(self):
        pass

    def _get_lastprice(self):
        self.lastquote = self.quote[self.quote.date==self.quote.date.max()].close.values
    
    def _get_quote(self):
        # load the entries for the current stock from the database
        cnx         = sqlite3.connect('database/stocks_quotes.db')
        sql         = pd.read_sql_query("SELECT * FROM quotes WHERE isin = '{0}';".format(self.isin), cnx)
        cnx.close()    
        if sql is not None:
            sql['date'] = sql.date.apply(convert_sql_date_to_datetime_date)
        self.quote  = sql
        
        self._get_lastprice()
        
    def get_highwatermark(self):
        '''Calculate the high watermark of the dataset'''
        hw = []
        mark = 0
        for i,j in self.quote.iterrows():
            if j['close']>mark and j['close']>0:
                mark = j['close']
            hw.append(mark)
        self.quote = self.quote.assign(highwatermark=pd.Series(hw))
        
class fundamentals:
    def __init__(self):
        pass

    def _get_keyratios(self):
        # load the entries for the current stock from the database
        cnx         = sqlite3.connect('database/stocks_keyratios.db')
        sql         = pd.read_sql_query("SELECT * FROM fundamentals WHERE ISIN = '{0}';".format(self.isin), cnx)
        cnx.close()    
        sql = sql.sort_values(by='year',ascending=False)
        sql = sql.reset_index(drop=True)
        
        self.keyratios = sql
        
    def _get_pe_for_year(self, pe_year, month=4, day=1, detailed=False):
        '''Get the price/earnings ratio for every day of the subsequent year'''
        # get the min/max date for quotes
        mindate = dt.date(pe_year+1, month, day)                                             
        maxdate = dt.date(pe_year+2, month, day)

        # get the eps for the year under consideration
        eps   = self.keyratios[self.keyratios.year==pe_year]['EarningsPerShare'].values[0]

        # extract the quote 
        quote = self.quote[(self.quote.date>mindate) & (self.quote.date<maxdate)]

        quote = quote.assign(pe=(quote.close/eps).values)
        quote = quote.assign(eps=np.ones(len(quote))*eps)

        if detailed:
            return quote
        return quote[['date','close','pe', 'eps']]
    
    def _get_all_pe(self,detailed=False):
        '''Creates an object with the price to earnings ratio for every day of all years'''
        finalquote = None
        year = self.keyratios.year.min()+1 

        for _ in range(len(self.keyratios.year)-1):
            if finalquote is None:
                finalquote = get_pe_for_year(year,detailed=detailed)
            else:
                finalquote = finalquote.append(get_pe_for_year(year,detailed=detailed),ignore_index=True)
            year+=1
        self.per_table = finalquote


# In[525]:

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
        self._add_result('OnlyPositiveEarnings', _col, point)
        
    def per_vs_historic_per(self,verbose=False):
        self._get_all_pe()
        
        _current = self.per_table[self.per_table.date == self.per_table.date.max()].pe.values[0]
        _limit1  = self.per_table.pe.quantile(0.33)
        _limit2  = self.per_table.pe.quantile(0.66)

        if _current<_limit1:
            point = 1
        elif _current>_limit2: 
            point = -1
        else:
            point = 0
            
        self._add_result('EPSvsHistoricEPS', '{0:0.1f}/{1:0.1f}: {2:0.1f}'.format(_limit1, _limit2, _current), point)
    
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
            
        _value = _roe.mean()
        self._add_result('HistoricROE', '{0:0.1f}'.format(_value), point)  
        
        
    def present_roe(self):
        '''Check if ROE is above 20%'''
        _roe = self.keyratios['ReturnonEquity']
        _val = _roe[0]

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

        if _val>12:
            point = 1
        elif _val<6:
            point = -1
        else:
            point = 0
            
        self._add_result('EBTMargin', '{0:0.1f}'.format(_val), point)  
        


# In[526]:

class time:
    '''Make the analysis for today a special case of backtesting'''
    
    def _assign_pointintime(self, day):
        # self.bdate = the backtesting date
        self.day      = day
        self.bdate    = self.quote.date[0] + dt.timedelta(days=self.day)
        self.quote    = self.quote[self.quote.date < self.bdate]
        
        if self.bdate.month<4:
            _max_keyratio_year = self.bdate.year - 2 
        else:
            _max_keyratio_year = self.bdate.year - 1

        self.keyratios = self.keyratios[self.keyratios['year'] < _max_keyratio_year]


# In[527]:

class stocks(quotes,fundamentals,algo,time):
    def __init__(self,stocklist,index=0,ISIN=None,verbose=False):
        
        self._initialize_algo()
        
        self.list    = stocklist
        self.verbose = verbose

        if ISIN:
            self.switch_isin(ISIN)
        else:
            self.switch_index(index)
        self._update_tables()
            
    def switch_isin(self,isin):
        df         = self.list[self.list.ISIN==isin]
        self.index = df.index[0]
        self.name, self.isin, self.ticker = np.array(df)[0]
        self._update_tables()
        
    def switch_index(self,index):
        self.index = index
        df         = self.list[self.list.index==index]
        self.name, self.isin, self.ticker = np.array(df)[0]
        
    def switch_next(self):
        try:
            self.switch_index(self.index+1)
            self._update_tables()
        except IndexError:
            pass

    def _update_tables(self):
        self._get_keyratios()
        self._get_quote()
        
    def update_time(self,day):
        '''This function resets the data such that only the data known at the 
        selected moment in time is available. '''
        self._update_tables()        
        self._assign_pointintime(day)




def main():
    s = stocks(xetra, verbose=True)

    # Point 1: check if all earnings were positive
    s.positive_earnings()

    # Point 2: Compare the historical P/E with the present p/e
    s.per_vs_historic_per()

    # Point 3: Check the historic RoE. Only give +1 if historic RoE was above 20% for all considered years [maybe change to max. 10 years]
    s.historic_roe()

    # Point 4: Check if the present RoE is > 20%
    s.present_roe()



        

# run the main part of the script

        
if __name__ == "__main__":
    # execute only if run as a script
    main()
