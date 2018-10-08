import pandas as pd
import numpy as np

from lib.Station_Crawler import Station_Crawler
from lib.Daily_Climate_Crawler import Daily_Climate_Crawler
from lib.Hourly_Climate_Crawler import Hourly_Climate_Crawler
from lib.db.csv_to_sql import csv_to_mssql

class Climate_Crawler:
	def __init__(self):
		self.to_sql = csv_to_mssql()
		# 抓氣候資料 instance
		self.daily_crawler = Daily_Climate_Crawler()
		self.hourly_crawler = Hourly_Climate_Crawler()
		# 爬蟲 log dataFrame
		self.log_df = self.to_sql.get_last_climate_crawler_log()
		# 設定新的 start 和 end period
		self.log_df = self.set_new_period(self.log_df)
		self.log_df_hourly = self.log_df.loc['hourly']
		self.log_df_daily = self.log_df.loc['daily']

	def start(self):
		if not ('Start_Period' in self.log_df.columns):
			# 如果 DB 為空，就抓三年前 2015-1-1 ~ 該天的昨天 期間的所有氣候資料
			self.get_climate_data_three_years_ago()
		else:
			self.has_crawler_log()

		self.to_sql.deal_with_daily_and_hourly_data()
		self.to_sql.disconnect()
		# 儲存爬蟲 log
		self.to_sql.save_climate_crawler_log(self.log_df)

	# 如果 DB 為空，就抓三年前 2015-1-1 ~ 該天的昨天 期間的所有氣候資料
	def get_climate_data_three_years_ago(self):
		print('如果 DB 為空，需要抓 三年前 2015-1-1 ~ 該天的昨天 期間的所有氣候資料')
		self.daily_crawler.obtain_daily_data(
			start_period='2015-01', end_period=self.log_df_daily['New_End_Period'])

		self.hourly_crawler.obtain_hourly_data(
			start_period='2015-01-01', end_period=self.log_df_hourly['New_End_Period'])

	def has_crawler_log(self):
		# 若 end_period + 1 天 == 今天 就代表 DB 已有最新的氣候資料
		has_latest_data = self.log_df_hourly['New_Start_Period'] == self.get_today_str()
		if (has_latest_data):
			print('已有最新資料，不必抓資料')
		else:
			self.get_recent_data()

	# 抓最新的氣候資料
	def get_recent_data(self):
		print('daily crawler: {} ~ {}'.format(
			self.log_df_daily['New_Start_Period'], self.log_df_daily['New_End_Period']))
		print('hourly crawler: {} ~ {}'.format(
			self.log_df_hourly['New_Start_Period'], self.log_df_hourly['New_End_Period']))

		self.daily_crawler.obtain_daily_data(
			start_period=self.log_df_daily['New_Start_Period'], end_period=self.log_df_daily['New_End_Period'], filter_period=self.log_df_hourly['New_Start_Period'])

		self.hourly_crawler.obtain_hourly_data(
			start_period=self.log_df_hourly['New_Start_Period'], end_period=self.log_df_hourly['New_End_Period'])

	# 設定新的 start 和 end period
	def set_new_period(self, log_df):
		if self.log_df is not None:
			log_df['New_Start_Period'] = log_df['End_Period'].apply(lambda period: self.add_one_day(period))
		else:
			log_df = pd.DataFrame({'Climate_Type': ['hourly', 'daily']})\
				.set_index('Climate_Type')

		today_time = pd.Timestamp.now()
		yesterday_time = today_time - pd.DateOffset(1)
		end_period_date = yesterday_time.date()
		log_df['New_End_Period'] = [end_period_date, end_period_date]

		log_df.loc['hourly'] = log_df.loc['hourly'].apply(lambda period: str(period))
		log_df.loc['daily'] = log_df.loc['daily'].apply(lambda period: str(period)[:-3])
		return log_df

	def add_one_day(self, date_str):
		return  (pd.Timestamp(date_str) + pd.DateOffset(1)).date()

	def get_today_str(self):
		today_time = pd.Timestamp.now()
		today_date = today_time.date()
		today_str = today_date.strftime('%Y-%m-%d')
		return today_str