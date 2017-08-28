import pandas as pd
import numpy as np
from pandas import DataFrame, HDFStore
import matplotlib.pyplot as plt
import datetime as dt
import sqlite3
import pandas_datareader.data as web
import os.path

import urllib2
import quandl as ql


# Loads the keyratios from morningstar

# load the stock databases

cnx        = sqlite3.connect('database/stocks_main.db')
xetra      = pd.read_sql("SELECT * FROM XETRA;", cnx)
currencies = pd.read_sql("SELECT * FROM currencies", cnx)

class keyratios:
    def __init__(self,filename):
        self.load(filename)
    
    def load_keyratios(self,filename):
        try:
            self.keyratios = pd.read_csv(filename,skiprows=2)
        except IOError:
            self.keyratios = None
            return
        self._correct_colnames()
        self.keyratios = self._format_keyratios()
    
    def _correct_colnames(self):
        newcols = []
        for i in self.keyratios.columns:
            if i == 'TTM':
                newcols.append(newcols[-1]+1)
                continue
            if 'Unnamed' in i:
                newcols.append('year')
                continue 
            newcols.append(int(i.split('-')[0]))
        self.keyratios.columns = newcols

    def _format_keyratios(self):
        pdf = self.keyratios
        pdf = pdf.transpose()
        
        cols_new     = []
        cols_to_use  = []

        cols_old = np.array(pdf[pdf.index=='year'])[0]
        
        currency = self._find_currency(cols_old)
        
        
        for string in cols_old:
    #         print string
            try:
                newstring = string
                newstring = newstring.replace(' ','')
                newstring = newstring.replace('%', '')
                newstring = newstring.replace('Mil','')
                newstring = newstring.replace('*','')
                newstring = newstring.replace('(','')
                newstring = newstring.replace(')','')

                for j in currencies.currency:
                    newstring = newstring.replace(j,'')
            except AttributeError:
                newstring = 'NaN'

            if newstring in cols_new:
                newstring = '{0}_{1}'.format(newstring,len(cols_new)+1)
            cols_new.append(newstring)    

        try:
            pdf.columns = cols_new
        except ValueError:
            return cols_old,cols_new
        pdf         = pdf[1:]


        for c in cols_new:
            if '_' not in c and '&' not in c:
                if c not in ['NaN','3-YearAverage','Efficiency','MarginsofSales','Growth',
                             'BalanceSheetItemsin','Liquidity/FinancialHealth','Profitability',
                             'CashFlowRatios']:
                    cols_to_use.append(c)


        pdf = pdf[cols_to_use]
        pdf = pdf.replace(to_replace=",",value="",regex=True)    


        pdf = pdf.astype('float')


        pdf = pdf[[
                    'Shares',
                    'Revenue',
                    'EarningsPerShare',
                    'Dividends',
                    'BookValuePerShare',
                    'OperatingCashFlow',
                    'ReturnonEquity',
                    'GrossMargin',
                    'OperatingIncome', 
                    'OperatingMargin',
                    'NetIncome',
                    'CapSpending',
                    'FreeCashFlow', 
                    'FreeCashFlowPerShare', 
                    'WorkingCapital', 
                    'COGS',
                    'EBTMargin', 
                    'TaxRate', 
                    'NetMargin',
                    'AssetTurnoverAverage', 
                    'ReturnonAssets',
                    'FinancialLeverageAverage',
                    'ReturnonInvestedCapital', 
                    'InterestCoverage' 
                   ]]

        pdf.currency = currency

        return pdf    

    def _find_currency(self,cols_old):
        for cur in currencies.currency:
            if 'Earnings Per Share {0}'.format(cur) in cols_old:
#                 print 'ISIN {0} found currency: {1}'.format(self.isin,cur)
                return cur


class quotes:
    def __init__(self):
        pass
    
    def _get_quote_quandl(self):
        isin, ticker = self.isin, self.ticker
        
        if isin[0:2]=='DE':
            prefix = "FSE/"
            suffix = "_X"
            key = "{0}{1}{2}".format(prefix,ticker,suffix)
            df  = ql.get(key, authtoken="CkyBkkkbzfh3ePfDF_ip")
            df  = df[['Open','High','Low','Close','Traded Volume']]
            df.columns = ['Open','High','Low','Close','Volume'] 
#             df.reset_index(level=0, inplace=True)
            
        elif isin[0:2]=='CH':
            key = "SIX/{0}CHF1".format(self.isin)
            df  = ql.get(key, authtoken="CkyBkkkbzfh3ePfDF_ip")            
            df.columns = ['Close','Volume']  
            
            
        elif isin[0:2]=='US':
            df = ql.get_table('WIKI/PRICES',ticker=ticker, paginate=True)
            df = df[['date','open','high','low','close','volume']]
            df.columns = ['Date','Open','High','Low','Close','Volume']
            
        if 'Date' not in df.index:
            df.reset_index(level=0, inplace=True)
            
        self.quote = df
        
    def _get_quote_google(self,isin,ticker):
        if isin[0:2]=='DE':
            prefix="FRA:"
        elif isin[0:2]=='CH':
            prefix="SIX"
        elif isin[0:2]=='US':
            prefix='NYSE'
            
        key = "{0}{1}".format(prefix,ticker)
        df  = web.DataReader(key, 'google', start, end)
        self.quote = df
    
    def _find_splits(self):
        relchange   = self.quote.Close.diff()/self.quote.Close
        self.splits = (relchange[relchange<-0.5]*(-1)+ 1).round()

        for i,splitdate in enumerate(self.quote.loc[self.splits.index].Date):
            self.quote.loc[self.quote.Date < 
                        splitdate,'Close'] = self.quote.loc[self.quote.Date < 
                                                         splitdate,'Close']/np.array(self.splits)[i]        
        
        
    def get_quote(self, find_splits=True):
        for func in [self._get_quote_quandl, self._get_quote_google]:
            try:
                func()
                break
            except:
                continue
        if not find_splits:
            return
        
        try:
            self._find_splits()
        except AttributeError:
            self.quote = None
            pass

class stocks(keyratios,quotes):
    def __init__(self,stocklist,index=0,ISIN=None,verbose=False):
        self.list  = stocklist

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
#         self._update_tables()
        
    def switch_next(self):
        try:
            self.switch_index(self.index+1)
            self._update_tables()
            self.quote = None
        except IndexError:
            pass

    def _update_tables(self):
        try:
            self.load_keyratios('ms_keyratios/'+self.ticker+'.csv')
        except:
            self.keyratios = None


weblink = 'http://financials.morningstar.com/ajax/exportKR2CSV.html?t=FB'

def save_keyratios(ticker,region,prefix):
    outfile = 'raw_data/ms_keyratios_07_16/{0}_{1}.csv'.format(prefix,ticker)
    
    if os.path.isfile(outfile):
        print 'EXISTING'
        return
    
    basestr = 'http://financials.morningstar.com/ajax/exportKR2CSV.html?'
    weblink = '{0}t={1}&region={2}&culture=en_US'.format(basestr,ticker,region)
    
    response = urllib2.urlopen(weblink)
    html     = response.read()

    
    with open(outfile,'w') as f:
        f.write(html)


        
country = {}
country['AU'] = 'AUS'
country['DE'] = 'DEU'
country['GR'] = 'DEU'
country['US'] = 'DEU'
country['FR'] = 'DEU'
country['CH'] = 'CH'
country['GB'] = 'GB'
country['JP'] = 'JP'

for i, entry in xetra.iterrows():
    if entry.ISIN[0:2] not in country.keys():
        continue
    print 'Downloading stock ISIN: {0} - {1}'.format(entry.ISIN, entry.Instrument)
    try:
        save_keyratios(entry.Mnemonic,country[entry.ISIN[0:2]],entry.ISIN[0:2])
    except:
        pass
