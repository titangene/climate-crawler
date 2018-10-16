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

def add_one_day(date_str):
	return (pd.Timestamp(date_str) + pd.DateOffset(1)).date()

def get_yesterday_date():
	today_time = pd.Timestamp.now()
	yesterday_time = today_time - pd.DateOffset(1)
	return yesterday_time.date()

def get_today_str():
	today_time = pd.Timestamp.now()
	today_date = today_time.date()
	today_str = today_date.strftime('%Y-%m-%d')
	return today_str