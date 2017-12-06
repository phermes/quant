import pandas as pd
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
import datetime as dt
import time as tt



def get_datetime(inputobj):
    return dt.datetime.date(inputobj)


def convert_sql_date_to_datetime_date(string):
    return dt.datetime.strptime(string,'%Y-%m-%d').date()

