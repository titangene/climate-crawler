import pandas as pd
import numpy as np

from configparser import ConfigParser, ExtendedInterpolation

class Config:
	def __init__(self):
		self.config = ConfigParser(interpolation=ExtendedInterpolation())
		self.config.read('lib/config/config.ini', encoding='utf-8')

	# 取得 資料庫連接 URL
	def get_db_url(self):
		return self.config['database']['db_url']

	def get_crawler_start_period_default(self):
		crawler_start_period_default = self.config['crawler_start_period']['default']
		default_setting_items = ['date', 'months', 'year']
		if crawler_start_period_default in default_setting_items:
			return crawler_start_period_default
		else:
			raise ValueError('config.ini 內的 [crawler_start_period] default 變數只能設定 date、months 或 year 其中一項')

	# 取得 從某天的資料時段開始擷取
	def get_crawler_start_date(self):
		crawler_start_date = self.config['crawler_start_period']['date']
		if pd.Timestamp(crawler_start_date) >= pd.Timestamp('2010-01-01'):
			return crawler_start_date
		else:
			raise ValueError('config.ini 內的 [crawler_start_period] date 變數必須是 >= 2010-01-01')

	# 取得 從幾個月前的資料時段開始擷取
	def get_crawler_started_a_few_months_ago(self):
		crawler_started_a_few_months_ago = self.config['crawler_start_period']['months']

		# 所有字元必須是數字，也就是必須是整數，不能有非數字的字元 (例如：小數點)
		if not crawler_started_a_few_months_ago.isdigit():
			raise ValueError('config.ini 內的 [crawler_start_period] months 變數必須是整數且 >= 1')

		if int(crawler_started_a_few_months_ago) >= 1:
			return int(crawler_started_a_few_months_ago)
		else:
			raise ValueError('config.ini 內的 [crawler_start_period] months 變數必須是整數且 >= 1')

	# 取得 從某年開始的資料時段開始擷取
	def get_crawler_starting_from_a_certain_year(self):
		crawler_start_year = self.config['crawler_start_period']['year']
		this_year = pd.Timestamp.now().year

		if int(crawler_start_year) >= 2010 and int(crawler_start_year) <= this_year:
			return crawler_start_year
		else:
			raise ValueError('config.ini 內的 [crawler_start_period] year 變數必須是 >= 2010，且 <= 今年')