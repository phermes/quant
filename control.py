#!/home/phermes/.conda/envs/py36/bin/python3.6

### INITIALIZE

import argparse
import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt
from stocks import stock
from stocks.output import logging, plotting



pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

# deal with arguments

parser = argparse.ArgumentParser()

parser.add_argument("-a", "--analyze", help="run the algo for all stocks", action="store_true")
parser.add_argument("-c", "--check",   help="show summary for individual stock")
parser.add_argument("-l", "--list",    help="list the top results from the analysis", action="store_true")
parser.add_argument("-q", "--quote",   help="downloads historical quotes and adds it to the database")
parser.add_argument("--optimistic",    help="Use earnings growth estimates in the calculation of the fair price. compatible with --check", action="store_true")
parser.add_argument("--noplot",   help="don't show the plot when performing --check", action="store_true")
parser.add_argument("--verbose",   help="activate verbose mode", action="store_true")
parser.add_argument("-r", "--remove",    help="remove quote for stock with given ISIN")

args = parser.parse_args()




# initialize the stock class
s          = stock(verbose=args.verbose)


if args.remove:
    s.verbose=True
    s.switch_isin(args.remove)    
    s.log_message("Removing quotes for stock {0}".format(s.name))
    s.log_message("DELETE FROM `quotes` WHERE `isin`='{0}';".format(args.remove))
    cnx        = sqlite3.connect('database/stocks_quotes.db')
    c = cnx.cursor()
    c.execute("DELETE FROM `quotes` WHERE `isin`='{0}';".format(args.remove))
    cnx.commit()
    cnx.close()

if args.quote:
    s.verbose=True
    s.switch_isin(args.quote)
    s.log_message("Downloading quotes for stock {0}".format(s.name))
    s._download_quote_yahoo()


if args.list:
    cnx        = sqlite3.connect('output/algo_results.db')
    results    = pd.read_sql("SELECT * FROM summary;", cnx)
    print("Listing Top Results\n")
    data = results
    print("Top Stocks Found: {0}/{1}\n".format(len(data[data['Points']>3]), len(data)))    
    data = data[data['Points']>3]
    data = data.assign(NPVpS =(data['FairPrice']-data['Price']))
    data = data.assign(Margin=(data['FairPrice']/data['Price'] -1 )*100)
    data = data.sort_values(['Points','Margin'],ascending=[False, False])
    print(data[['Name', 'ISIN', 'FairPrice', 'FairPricePE', 'Price', 'NPVpS', 'Margin', 'Points']])
    


if args.analyze:
    print("analyzing...")

if args.check:
    s.switch_isin(args.check)
    print("Checking stock {0}".format(s.name))
    if args.optimistic:
        conservative=False
    else:
        conservative=True
    s.get_summary(save=True,conservative=conservative)
    s.analyze_quote()
    print("\n   #### OVERVIEW ####\n")
    print("   Name                        {0}".format(s.name))
    print("   ISIN                        {0}".format(s.isin))
    print("   Quote                      {0: 1.2f}".format(s.price))
    print("   Fair Price (Lower limit)   {0: 1.2f}".format(s.fairprice_low))
    print("   Fair Price (Upper limit)   {0: 1.2f}".format(s.fairprice_high))
    print("   Potential Gain (%)         {0: 1.2f}".format(100*(s.fairprice_low-s.price)/(s.price)))
    print("   Volatility (Daily/Yearly)  {0: 1.3f}/{1:1.3f}".format(s.volatility_day, s.volatility_year))
    print('\n   #### ANALYSIS ####\n')
    print(s.summary)
    print(s.quant_result)
    
    input("Press Enter to continue to interactive summary...")

    if not args.noplot:
        s.interactive_summary()
        tt.sleep(5)
    

    
