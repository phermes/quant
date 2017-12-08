import pandas as pd
import numpy as np
from pandas import DataFrame, HDFStore
import matplotlib.pyplot as plt
import datetime as dt
import sqlite3
import pandas_datareader.data as web


from stocks import stock

### MAIN

s = stock(verbose=True)
# s.switch_isin('DE0007757007')

# while s.isin[0:2]=='DE':
for _ in range(len(s.list)):
	s._download_quote_yahoo()
	s.switch_next()
