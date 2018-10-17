import pandas as pd
import numpy as np

import lib.Climate_Common as Climate_Common
from lib.Climate_Station import Climate_Station
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

		self.climate_station = Climate_Station()
		self.station_id_list = self.climate_station.station_id_list
		# 抓氣候資料 instance
		self.daily_crawler = Daily_Climate_Crawler(self.climate_station)
		self.hourly_crawler = Hourly_Climate_Crawler(self.climate_station)

	def start(self):
		if self.log_df.empty:
			# 如果 DB 為空，就抓三年前 2015-1-1 ~ 該天的昨天 期間的所有氣候資料
			self.get_climate_data_three_years_ago()
			is_catch_data = True
		else:
			if self.is_latest_data():
				print('已有最新資料，不必抓新資料')
				is_catch_data = False
			else:
				# 抓最新的氣候資料 (抓爬蟲 log 之後的新資料)
				self.log_df = self.get_recent_climate_data(self.log_df)
				is_catch_data = True

		if is_catch_data:
			# 更新爬蟲 log dataFrame
			self.log_df = self.climate_crawler_Log.update_log_dataFrame(self.log_df)
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

	# 是否有最新的氣候資料
	def is_latest_data(self):
		is_hourly_latest_data = Climate_Common.is_today(self.log_df['New_Hourly_Start_Period']).all()
		is_daily_latest_data = Climate_Common.is_today(self.log_df['New_Daily_Start_Period']).all()
		print('hourly 是否有最新資料:', is_hourly_latest_data, '| daily 是否有最新資料:', is_daily_latest_data)
		return is_hourly_latest_data and is_daily_latest_data

	def get_recent_climate_data(self, log_df):
		for station_id, row in log_df.iterrows():
			print(station_id, row['Station_Area'])

			# 計算爬蟲要抓資料的時間範圍
			# e.g. 將 '2018-10-05' 變成 '2018-10'，只取年月
			daily_start_period = row['New_Daily_Start_Period'][:-3]
			daily_end_period = row['New_Daily_End_Period'][:-3]
			hourly_start_period = row['New_Hourly_Start_Period']
			hourly_end_period = row['New_Hourly_End_Period']

			daily_periods = Climate_Common.get_month_periods(daily_start_period, daily_end_period)
			hourly_periods = Climate_Common.get_day_periods(hourly_start_period, hourly_end_period)
			print('daily periods:', daily_periods)
			print('hourly periods:', hourly_periods)

			filter_period = row['New_Daily_Start_Period']

			daily_record_start_period, daily_record_end_period = \
					self.daily_crawler.get_station_climate_data(station_id, daily_periods, filter_period)
			hourly_record_start_period, hourly_record_end_period = \
					self.hourly_crawler.get_station_climate_data(station_id, hourly_periods)
			print('record daily crawler: {} ~ {}'.format(daily_record_start_period, daily_record_end_period))
			print('record hourly crawler: {} ~ {}\n'.format(hourly_record_start_period, hourly_record_end_period))

			row['Reporttime'] = pd.Timestamp.now()
			row['New_Daily_Start_Period'] = daily_record_start_period
			row['New_Daily_End_Period'] = daily_record_end_period
			row['New_Hourly_Start_Period'] = hourly_record_start_period
			row['New_Hourly_End_Period'] = hourly_record_end_period

			log_df.loc[station_id] = row
		return log_df