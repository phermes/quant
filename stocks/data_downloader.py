import bs4
from bs4 import BeautifulSoup
import requests
import sqlite3
import pandas as pd
import datetime 
from stocks.quotes import convert_sql_date_to_datetime_date
import numpy as np

class quarterly_report:
	def __init__(self):
		pass

	def _find_quarterly_report_dates_table(self,string):
		'''Returns the table containing dates for the quarterly report'''
		tables = pd.read_html('http://www.finanzen.ch/termine/{0}'.format(string))
		self.log_message("Downloading quarterly report dates from http://www.finanzen.net/termine/{0}".format(string))

		for table in tables:
			try:
				if 'Quartalszahlen' in table[0].values:
					self.debug_message("Table before cleaning")
					self.debug_message(table)
					table = table[table[0]=='Quartalszahlen']
					self.log_message("Found quarterly report table with {0} entries".format(len(table)))
					return table
			except KeyError:
					continue
		return None

	def _read_stored_quarterly_report_dates(self):
		'''Load the stored quarterly report date for the current stock from the database.'''

		self.log_message("Reading saved quarterly report dates")
		cnx          = sqlite3.connect('database/stocks_dates.db')
		dates_saved  = pd.read_sql_query("SELECT * FROM quarterly_report WHERE isin = '{0}';".format(self.isin), cnx)
		cnx.close()

		# convert date strings to datetime objects
		dates_saved['report_date'] = dates_saved['report_date'].apply(convert_sql_date_to_datetime_date)

		self._quarterly_report_dates_saved = dates_saved	
		self.quarterly_report_dates        = dates_saved                  

	def _save_unsaved_quarterly_report_dates(self):
		'''Find entries in the downloaded dataset which are not in the saved dataset'''

		# download the quarterly report dates from the web
		self._get_quarterly_report_dates()

		# read the stored quarterly report dates stored in the database
		self._read_stored_quarterly_report_dates()

		if self._quarterly_report_dates_downloaded is None:
			self.log_message("Could not download quarterly report dates")
			return 

		# compare the dates between downloaded and saved dates
		s1       = self._quarterly_report_dates_downloaded['report_date']
		s2       = self._quarterly_report_dates_saved['report_date']
		newdates = pd.Series(np.setdiff1d(s1.values,s2.values))
		newdates = newdates.values

		# extract the lines to save =  dates in newdates
		quarterly_report_dates_to_save = self._quarterly_report_dates_downloaded[self._quarterly_report_dates_downloaded['report_date'].isin(newdates)]

		self.log_message("Found {0} quarterly report dates to save".format(len(quarterly_report_dates_to_save)))
		# self._quarterly_report_dates_to_save = quarterly_report_dates_to_save

		# save the lines in the sqlite database
		if quarterly_report_dates_to_save is None:
			self.log_message("No quarterly report dates to save")
			return

		self.debug_message("Saving to database/stocks_dates.db")   
		cnx         = sqlite3.connect('database/stocks_dates.db')
		quarterly_report_dates_to_save.to_sql('quarterly_report',cnx,if_exists='append',index=False)
		cnx.close()    
        
		self.log_message("Successfully saved {0} entries in quarterly report database".format(len(quarterly_report_dates_to_save)))

		# update self.quarterly_report_dates
		self._read_stored_quarterly_report_dates()


	def _get_quarterly_report_dates(self):
		'''Returns a dataframe containing the last quarterly reports'''
		table = self._find_quarterly_report_dates_table(self._fn_link)

	    # return if table is empty
		if table is None:
			self._quarterly_report_dates_downloaded = None
			return

	    # transform the date strings into datetime objects
		output = []
		for column in table.columns:
			for element in table[column]:
				try:
					output.append(datetime.datetime.strptime(element, "%d.%m.%y").date())
				except:
					pass
		output = [(self.name, self.isin, d) for d in output]
		output = pd.DataFrame(output,columns=['name','isin','report_date'])

		self._quarterly_report_dates_downloaded = output