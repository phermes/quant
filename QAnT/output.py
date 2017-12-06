import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt


class logging:
    '''Error handling for the algoritm'''
    def _get_timestamp(self):
        ts = tt.time()
        return dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    def error_message(self,message):
        
        ts = self._get_timestamp()
        text_file = open("output/algo.err", "a")

        isin_ticker = self._get_isin_ticker()

        _output      = "{0}  {1:12s}  {2:17s}|err|  {3}".format(ts, isin_ticker, self.name[0:17], message)
        
        text_file.write(_output+ "\n")
        
        if self.verbose:
            print(_output)        
        
        text_file.close()

    def log_message(self,message,logtype='|log|'):
        ts = self._get_timestamp()
        text_file = open("output/algo.log", "a")

        isin_ticker = self._get_isin_ticker()

        _output   = "{0}  {1:12s}  {2:17s} {3}  {4}".format(ts, isin_ticker, self.name[0:17], logtype, message)        
        text_file.write(_output + "\n")
        if self.verbose:
            print(_output)
        text_file.close()        

    def _get_isin_ticker(self):
        if self._type == "index":
            isin_ticker = self.ticker
        else:
            isin_ticker = self.isin
        return isin_ticker

    def debug_message(self,message):
        if not self.debug:
            return
        self.log_message(message,logtype='|deb|')



class plotting:
    '''Plot key quantities'''
    
    def plot_summary(self):
            # plot with various axes scales
        plt.figure(8,figsize=(15,28))

        nrows=800
        ncols=20
        # linear

        plt.subplot(nrows+ncols+1)
        plt.plot(self.quote['date'],self.quote['close'])
        plt.xlabel('Date')
        plt.ylabel('Price')

        plt.subplot(nrows+ncols+2)
        plt.plot(self.keyratios['year'], self.keyratios['NetIncome'],'o-')
        plt.xlabel('Date')
        plt.ylabel('Net Income [Mio.]')

        plt.subplot(nrows+ncols+3)
        plt.plot(self.keyratios['year'], self.keyratios['EBTMargin'],'o-')
        plt.xlabel('Date')
        plt.ylabel('EBT Margin [%]')
        plt.axhspan(0, 6, facecolor='red', alpha=0.1)        
        plt.axhspan(6, 12, facecolor='yellow', alpha=0.1)
        plt.axhspan(12, 100, facecolor='green', alpha=0.1)
        plt.ylim(self.keyratios['EBTMargin'].min()*0.9, self.keyratios['EBTMargin'].max()*1.1)

        plt.subplot(nrows+ncols+4)
        plt.plot(self.keyratios['year'], self.keyratios['BookValuePerShare'],'o-')
        plt.xlabel('Date')
        plt.ylabel('Book value per Share')


        # ReturnonEquity

        plt.subplot(nrows+ncols+5)
        plt.plot(self.keyratios['year'], self.keyratios['ReturnonEquity'],'o-')
        plt.xlabel('Date')
        plt.ylabel('Return on Equity [%]')
        plt.axhspan(10, 20, facecolor='yellow', alpha=0.1)
        plt.axhspan(20, 100, facecolor='green', alpha=0.1)
        plt.ylim(self.keyratios['ReturnonEquity'].min()*0.9, self.keyratios['ReturnonEquity'].max()*1.1)


        # TotalShareholdersEquity

        plt.subplot(nrows+ncols+6)
        plt.plot(self.keyratios['year'], self.keyratios['TotalStockholdersEquity'],'o-')
        plt.xlabel('Date')
        plt.ylabel('Equity Ratio [%]')
        plt.axhspan(0, 15,   facecolor='yellow', alpha=0.1)        
        plt.axhspan(15, 25,  facecolor='yellow', alpha=0.1)
        plt.axhspan(25, 100, facecolor='green',  alpha=0.1)
        plt.ylim(self.keyratios['TotalStockholdersEquity'].min()*0.9, self.keyratios['TotalStockholdersEquity'].max()*1.1)



        plt.subplot(nrows+ncols+7)
        plt.plot(self.per_table['date'], self.per_table['pe'],'.')
        plt.axhline(self.per_table['pe'][0], 0,1,color='black')
        plt.xlabel('Date')
        plt.ylabel('P/E ratio')

        plt.subplot(nrows+ncols+8)
        s.per_table['pe'].hist(bins=100)
        plt.axvline(self.per_table['pe'][0], 0,1,color='black')
        plt.ylabel('Abundance')
        plt.xlabel('P/E ratio')
        plt.show()
        
    def interactive_summary(self):
        '''Show an interactive summary'''

        if self.quote is not None:
            trace1  = go.Scatter(x=self.quote['date'],     y=self.quote['close'], 
                mode = 'markers',           name='Price')

            fairprm = go.Scatter(
                                    x=[self.quote['date'].min(),self.quote['date'].max()],
                                    y=[self.fairprice, self.fairprice],
                                    mode='lines',
                                    name="Mean fair price")
            fairprl = go.Scatter(
                                    x=[self.quote['date'].min(),self.quote['date'].max()],
                                    y=[self.fairprice_low, self.fairprice_low],
                                    mode='lines',
                                    name="Min. fair price")

            

        

        netincome = go.Scatter(x=self.keyratios['year'], y=self.keyratios['NetIncome'],    name='Net Income')
        freecf = go.Scatter(x=self.keyratios['year'], y=self.keyratios['FreeCashFlow'], name='Free Cash Flow')
        trace6 = go.Scatter(x=self.keyratios['year'], 
                            y=self.keyratios['BookValuePerShare'], 
                            name='Book value per Share')
        ebtmargin = go.Scatter(x=self.keyratios['year'], y=self.keyratios['EBTMargin'], name='EBT Margin', yaxis='y26')
        roe       = go.Scatter(x=self.keyratios['year'],
                               y=self.keyratios['ReturnonEquity'],
                               name='Return on Equity',
                               yaxis='y23')

        equityratio = go.Scatter(x=self.keyratios['year'],
                                 y=self.keyratios['TotalStockholdersEquity'],
                                 name='Equity Ratio')

        bookvalue  = go.Scatter(x=self.keyratios['year'], y=self.keyratios['BookValuePerShare'],    name='Book Value')
        dividend   = go.Scatter(x=self.keyratios['year'], y=self.keyratios['Dividends'],    name='Dividend')



        if self.per_table is not None:
            trace7 = go.Scatter(
                x=self.per_table['date'],
                y=self.per_table['pe'],
                name='P/E ratio',
                mode = 'markers'
            )

            trace7line = go.Scatter(
                                    x=[self.per_table['date'].min(),self.per_table['date'].max()],
                                    y=[self.per_table['pe'].median(),self.per_table['pe'].median()],
                                    mode='lines',
                                    name="Median P/E")


            # trace8 = go.Histogram(
            #     x=self.per_table['pe'],
            #     nbinsx=200)

            
            # trace8line = go.Scatter(y=[0,50],
            #                         x=[self.per_table['pe'][0],self.per_table['pe'][0]],
            #                         mode='lines',
            #                         name="Current P/E")




        
        fig = plotly.tools.make_subplots(rows=4, cols=2)

        fig['layout'].update({'yaxis23': go.YAxis(
                              anchor='x3',
                              overlaying='y3',
                              side='right',
                              title='RoE [%]')})


        fig['layout']['xaxis1'].update(title='Date', showgrid=True)
        if self.quote is not None:
            fig['layout']['yaxis1'].update(title='Closing Price [{0}]'.format(self.quote['currency'][0]))

        fig['layout']['xaxis2'].update(title='Year', showgrid=True)
        fig['layout']['yaxis2'].update(title='Net Income & Free Cashflow [Mio. {0}]'.format(self.keyratios['currency'][0]))

        fig['layout']['xaxis3'].update(title='Year', showgrid=True)
        fig['layout']['yaxis3'].update(title='Equity Ratio [%]')

        fig['layout']['xaxis4'].update(title='Year', showgrid=True)
        fig['layout']['yaxis4'].update(title='EBT Margin [%]')

        fig['layout']['xaxis5'].update(title='Year', showgrid=True)
        fig['layout']['yaxis5'].update(title='P/E Ratio')

        # fig['layout']['xaxis6'].update(title='P/E Ratio', showgrid=True)
        # fig['layout']['yaxis6'].update(title='Abundance')

        fig['layout']['xaxis6'].update(title='Year', showgrid=True)
        fig['layout']['yaxis6'].update(title='Book Value Per Share [{0}]'.format(self.keyratios['currency'][0]))      

        fig['layout']['xaxis7'].update(title='Year', showgrid=True)
        fig['layout']['yaxis7'].update(title='Dividend [{0}]'.format(self.keyratios['currency'][0]))          

        if self.quote is not None:
            fig.append_trace(trace1, 1, 1)
            fig.append_trace(fairprm, 1, 1)
            fig.append_trace(fairprl, 1, 1)
            # fig.append_trace(fairprh, 1, 1)

            fig.append_trace(trace7, 3, 1)
            fig.append_trace(trace7line, 3, 1)
            # fig.append_trace(trace8, 3, 2)
            # fig.append_trace(trace8line, 3, 2)

        # second plot [net income and free cash flow]
        fig.append_trace(netincome, 1, 2)
        fig.append_trace(freecf,    1, 2)

        fig.append_trace(roe, 2, 1)
        fig.append_trace(equityratio, 2, 1)

        fig.append_trace(ebtmargin, 2, 2)

        fig.append_trace(bookvalue, 3,2)
        fig.append_trace(dividend, 4,1)

        fig['data'][7].update(yaxis='y23')

        fig['layout'].update(height=2200, width=1800, title='Stock {0}, ISIN {1}'.format(self.name,self.isin))
        plotly.offline.plot(fig)
