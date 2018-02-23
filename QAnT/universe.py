import pandas as pd
import sqlite3



class universe:
    def self.__init__():
        self.get_universe_value_at_risk()

    def get_universe_value_at_risk():
        '''Returns a DataFrame containing the value at risk of all stocks in the universe'''
        connection = sqlite3.connect('database/stocks_risk.db')
        query_output = pd.read_sql_query("SELECT * FROM value_at_risk;", connection)
        connection.close()
        query_output = query_output[query_output['return']<1]
        self.value_at_risk = query_output
