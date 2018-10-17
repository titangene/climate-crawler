import pandas as pd
import numpy as np

import lib.Climate_Common as Climate_Common
from lib.Daily_Climate_Crawler import Daily_Climate_Crawler
from lib.Hourly_Climate_Crawler import Hourly_Climate_Crawler
from lib.db.csv_to_sql import csv_to_mssql
from lib.Climate_Crawler_Log import Climate_Crawler_Log

class Climate_Crawler:
	def __init__(self):
		self.to_mssql = csv_to_mssql()

		# 爬蟲 log instance
		self.climate_crawler_Log = Climate_Crawler_Log(self.to_mssql)
		self.log_df = self.climate_crawler_Log.log_df

		# 抓氣候資料 instance
		self.daily_crawler = Daily_Climate_Crawler()
		self.hourly_crawler = Hourly_Climate_Crawler()

	def start(self):
		if self.log_df.empty:
			# 如果 DB 為空，就抓三年前 2015-1-1 ~ 該天的昨天 期間的所有氣候資料
			self.get_climate_data_three_years_ago()
		else:
			self.is_crawler_log()

		# 更新爬蟲 log dataFrame
		self.log_df = self.climate_crawler_Log.update_dataFrame(self.log_df)
		# 儲存爬蟲 log
		self.climate_crawler_Log.save_climate_crawler_log(self.log_df)
		# 儲存 日 和 小時 氣候資料
		self.to_mssql.save_daily_and_hourly_data()
		# 關閉資料庫連線
		self.to_mssql.disconnect()

	# 如果 DB 為空，就抓三年前 2015-1-1 ~ 該天的昨天 期間的所有氣候資料
	def get_climate_data_three_years_ago(self):
		print('如果 DB 為空，需要抓 三年前 2015-1-1 ~ 該天的昨天 期間的所有氣候資料')
		daily_start_period = '2015-01'
		hourly_start_period = '2015-01-01'
		end_period = Climate_Common.get_yesterday_date()

		self.daily_crawler.obtain_data(
				start_period=daily_start_period, end_period=end_period)

		self.hourly_crawler.obtain_data(
				start_period=hourly_start_period, end_period=end_period)

	def is_crawler_log(self):
		if self.is_latest_data():
			print('已有最新資料，不必抓資料')
		else:
			# 抓最新的氣候資料
			self.log_df = self.climate_crawler_Log.get_recent_data(self.log_df, self.daily_crawler, self.hourly_crawler)

	# 是否有最新的氣候資料
	def is_latest_data(self):
		is_hourly_latest_data = Climate_Common.is_today(self.log_df['New_Hourly_Start_Period']).all()
		is_daily_latest_data = Climate_Common.is_today(self.log_df['New_Daily_Start_Period']).all()
		print('hourly 是否有最新資料:', is_hourly_latest_data, '| daily 是否有最新資料:', is_daily_latest_data)
		return is_hourly_latest_data and is_daily_latest_data
