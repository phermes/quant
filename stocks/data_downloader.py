import bs4
from bs4 import BeautifulSoup
import requests
import sqlite3
import pandas as pd
import datetime 


class quartery_report:
	def __init__(self):
		pass


	def _find_quartery_report_dates_table(self,string):
		'''Returns the table containing dates for the quartery report'''
		tables = pd.read_html('http://www.finanzen.ch/termine/{0}'.format(string))
		self.log_message("Downloading quartery report dates from http://www.finanzen.net/termine/{0}".format(string))

		for table in tables:
			try:
				if 'Quartalszahlen' in table[0].values:
					self.debug_message("Table before cleaning")
					self.debug_message(table)
					table = table[table[0]=='Quartalszahlen']
					self.log_message("Found quartery report table with {0} entries".format(len(table)))
					return table
			except KeyError:
					continue
		return None
	    

	def get_quartery_report_dates(self):
		'''Returns a dataframe containing the last quartery reports'''
		table = self._find_quartery_report_dates_table(self.fn_link)

	    # return if table is empty
		if table is None:
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
		return pd.DataFrame(output,columns=['name','isin','report_date'])