import pandas as pd
import numpy as np

from lib.config.config import Config

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
	return add_one_time.strftime('%Y-%m-%d')

def get_yesterday_date_str():
	today_time = pd.Timestamp.now()
	yesterday_time = today_time - pd.DateOffset(1)
	return yesterday_time.strftime('%Y-%m-%d')

def get_today_str():
	today_time = pd.Timestamp.now()
	return today_time.strftime('%Y-%m-%d')

# 取得 擷取近期的氣候資料的起始時段
def get_recent_climate_data_start_period():
	config = Config()
	crawler_start_default = config.get_crawler_start_period_default()

	if crawler_start_default == 'date':
		crawler_start_date = config.get_crawler_start_date()
		return pd.Timestamp(crawler_start_date).strftime('%Y-%m-%d')
	elif crawler_start_default == 'months':
		today_time = pd.Timestamp.now()
		crawler_started_a_few_months_ago = config.get_crawler_started_a_few_months_ago()
		started_a_few_months_ago = today_time - pd.DateOffset(months=crawler_started_a_few_months_ago)
		return started_a_few_months_ago.strftime('%Y-%m-%d')
	else:
		crawler_start_year = config.get_crawler_starting_from_a_certain_year()
		return '{}-01-01'.format(crawler_start_year)

def get_current_time():
	today_time = pd.Timestamp.now()
	return today_time.strftime('%Y-%m-%d-%H-%M-%S')