import pandas as pd
import numpy as np

# e.g. get_month_periods(start_period='2017-11', end_period='2018-2')
# output: ['2017-11', '2017-12', '2018-01']
def get_month_periods(start_period, end_period):
	return pd.date_range(start=start_period, end=end_period, freq='MS').strftime('%Y-%m').tolist()

# e.g. get_day_periods(start_period='2017-12-30', end_period='2018-01-02')
# output: ['2017-12-30', '2017-12-31', '2018-01-01', '2018-01-02']
def get_day_periods(start_period, end_period):
	return pd.date_range(start=start_period, end=end_period, freq='D').strftime('%Y-%m-%d').tolist()

def is_today(date_str):
	return date_str == get_today_str()

def add_one_day_str(date_str):
	add_one_time = pd.Timestamp(date_str) + pd.DateOffset(1)
	return add_one_time.date().strftime('%Y-%m-%d')

def get_yesterday_date_str():
	today_time = pd.Timestamp.now()
	yesterday_time = today_time - pd.DateOffset(1)
	return yesterday_time.date().strftime('%Y-%m-%d')

def get_today_str():
	today_time = pd.Timestamp.now()
	return today_time.date().strftime('%Y-%m-%d')

def get_begin_three_years_ago():
	today_time = pd.Timestamp.now()
	begin_three_years_ago = today_time.year - 3
	return '{}-01-01'.format(begin_three_years_ago)