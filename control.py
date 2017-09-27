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
parser.add_argument("--conservative",   help="switch off growth estimates in the calculation of the fair price. compatible with --check", action="store_true")
parser.add_argument("--noplot",   help="don't show the plot when performing --check", action="store_true")
parser.add_argument("--verbose",   help="activate verbose mode", action="store_true")

args = parser.parse_args()




# initialize the stock class
s          = stock(verbose=args.verbose)


if args.quote:
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
    if args.conservative:
        conservative=True
    else:
        conservative=False
    s.get_summary(save=True,conservative=conservative)

    print("Result:\n")
    print(s.summary)
    print(s.quant_result)
    
    input("Press Enter to continue to interactive summary...")

    if not args.noplot:
        s.interactive_summary()
        tt.sleep(5)
    

    
