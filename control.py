#!/usr/bin/python3.5m

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

cnx        = sqlite3.connect('database/stocks_main.db')
xetra      = pd.read_sql("SELECT * FROM XETRA;", cnx)
s          = stock(xetra,verbose=True)


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
parser.add_argument("--conservative",   help="switch off growth estimates in the calculation of the fair price. compatible with --check", action="store_true")
parser.add_argument("--noplot",   help="don't show the plot when performing --check", action="store_true")

args = parser.parse_args()



if args.quote:
    s.switch_isin(args.quote)
    print("Downloading quotes for stock {0}".format(s.name))
    s.get_quote(download=True,save=True,verbose=True)


if args.list:
    cnx        = sqlite3.connect('output/algo_results.db')
    results    = pd.read_sql("SELECT * FROM summary;", cnx)
    print("Listing Top Results\n")
    data = results.sort_values('Points',ascending=False)
    print("Top Stocks Found: {0}/{1}\n".format(len(data[data['Points']>3]), len(data)))
    data = data[data['Points']>3]
    data = data.assign(Margin=(data['FairPrice']/data['Price'] -1 )*100)
    print(data[['Name', 'ISIN', 'FairPrice', 'FairPricePE', 'Price', 'Margin', 'Points']])
    


if args.analyze:
    print("analyzing...")

if args.check:
    s.switch_isin(args.check)
    print("Checking stock {0}".format(s.name))
    if args.conservative:
        conservative=True
    else:
        conservative=False
    s.get_summary(save=False,conservative=conservative)
    
    if not args.noplot:
        s.interactive_summary()
        tt.sleep(5)
    print("Result:\n")
    print(s.summary)
    print(s.quant_result)
    

    
