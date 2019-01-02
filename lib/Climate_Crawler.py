import logging

import pandas as pd
import numpy as np

import lib.Climate_Common as Climate_Common
from lib.Climate_Station import Climate_Station
from lib.Daily_Climate_Crawler import Daily_Climate_Crawler
from lib.Hourly_Climate_Crawler import Hourly_Climate_Crawler
from lib.db.csv_to_sql import csv_to_mssql
from lib.Climate_Crawler_Log import Climate_Crawler_Log
from lib.csv.csv_process import merge_climate_data_to_csv
from lib.Logging import Logging

class Climate_Crawler:
	def __init__(self):
		self.to_mssql = csv_to_mssql()

		self.climate_crawler_Log = Climate_Crawler_Log(self.to_mssql)
		self.log_df = self.climate_crawler_Log.log_df

		self.climate_station = Climate_Station()
		self.station_df = self.climate_station.station_df

		self.daily_crawler = Daily_Climate_Crawler(self.climate_station, self.to_mssql)
		self.hourly_crawler = Hourly_Climate_Crawler(self.climate_station, self.to_mssql)

		self.recent_climate_data_daily_start_period = Climate_Common.get_recent_climate_data_start_period()[:-3]
		self.recent_climate_data_hourly_start_period = Climate_Common.get_recent_climate_data_start_period()

		Logging().setting()

	def start(self):
		if not self.log_df.empty and self.is_latest_data():
			print('已有最新資料，不必擷取新資料')
			return

		print('\n# init log_df:')
		print(self.log_df, '\n')

		# 擷取氣候資料
		self.log_df = self.get_climate_data(self.log_df)
		print('\n# get_climate_data:')
		print(self.log_df)

		# 更新爬蟲 log dataFrame
		self.log_df = self.climate_crawler_Log.update_log_dataFrame(self.log_df)
		print('\n# update_log_dataFrame:')
		print(self.log_df)

		# 儲存爬蟲 log
		self.climate_crawler_Log.save_log(self.log_df)
		print('')

		# 合併氣候資料
		# return: 合併氣候資料是否成功 (type: bool)
		is_merge_daily_climate, is_merge_hourly_climate = merge_climate_data_to_csv()
		print('daily_climate merge Success:', is_merge_daily_climate)
		print('hourly_climate merge Success:', is_merge_hourly_climate)

		# 關閉資料庫連線
		self.to_mssql.disconnect()

	# 是否有最新的氣候資料
	def is_latest_data(self):
		is_hourly_latest_data = Climate_Common.is_today(self.log_df['New_Hourly_Start_Period']).all()
		is_daily_latest_data = Climate_Common.is_today(self.log_df['New_Daily_Start_Period']).all()
		print('hourly 是否有最新資料:', is_hourly_latest_data, '| daily 是否有最新資料:', is_daily_latest_data)
		return is_hourly_latest_data and is_daily_latest_data

	# 擷取氣候資料
	def get_climate_data(self, log_df):
		for station_id, row in self.station_df.iterrows():
			# 爬蟲 log 內是否有此 station_id 紀錄
			if station_id in log_df.index:
				print('# with crawler log:', station_id, row['station_area'], '==================')
				temp_row = log_df.loc[station_id]
				print('old last daily:', temp_row['Daily_End_Period'], '| old last hourly:', temp_row['Hourly_End_Period'])
				# 擷取最新的氣候資料
				temp_row = self.get_latest_climate_data(temp_row, station_id)
			else:
				print('# without crawler log:', station_id, row['station_area'], '==================')
				station_area = row['station_area']
				# 擷取近期的氣候資料
				temp_row = self.get_recent_climate_data(station_id, station_area)

			if temp_row is not None:
				log_df.loc[station_id] = temp_row
			print('\n# log_df:')
			print(log_df)
			print('\n===============================\n')
		return log_df

	# 擷取最新的氣候資料：
	# 擷取爬蟲 log 紀錄之後所要擷取的新氣候資料
	def get_latest_climate_data(self, temp_row, station_id):
		temp_row = temp_row.copy()
		# 計算爬蟲要擷取資料的時段
		# e.g. 將 '2018-10-05' 變成 '2018-10'，只取年月
		daily_start_period = temp_row['New_Daily_Start_Period'][:-3]
		daily_end_period = temp_row['New_Daily_End_Period'][:-3]
		hourly_start_period = temp_row['New_Hourly_Start_Period']
		hourly_end_period = temp_row['New_Hourly_End_Period']

		# 取得要擷取的資料時段 (包括天、小時)
		daily_periods = Climate_Common.get_month_periods(daily_start_period, daily_end_period)
		hourly_periods = Climate_Common.get_day_periods(hourly_start_period, hourly_end_period)
		print('daily periods:', daily_periods)
		print('hourly periods:', hourly_periods)

		filter_period = temp_row['New_Daily_Start_Period']
		print('filter period:', filter_period)

		# 擷取氣候資料
		daily_record_start_period, daily_record_end_period = \
				self.daily_crawler.get_station_climate_data(station_id, daily_periods, filter_period)
		hourly_record_start_period, hourly_record_end_period = \
				self.hourly_crawler.get_station_climate_data(station_id, hourly_periods)
		print('record daily crawler: {} ~ {}'.format(daily_record_start_period, daily_record_end_period))
		print('record hourly crawler: {} ~ {}\n'.format(hourly_record_start_period, hourly_record_end_period))

		is_daily_record_period = self.is_record_period(daily_record_start_period, daily_record_end_period)
		is_hourly_record_period = self.is_record_period(hourly_record_start_period, hourly_record_end_period)
		print('is_daily_record_period:', is_daily_record_period)
		print('is_hourly_record_period:', is_hourly_record_period)

		if is_daily_record_period:
			temp_row['New_Daily_Start_Period'] = daily_record_start_period
			temp_row['New_Daily_End_Period'] = daily_record_end_period

		if is_hourly_record_period:
			temp_row['New_Hourly_Start_Period'] = hourly_record_start_period
			temp_row['New_Hourly_End_Period'] = hourly_record_end_period

		if is_daily_record_period or is_hourly_record_period:
			temp_row['Reporttime'] = pd.Timestamp.now()

		return temp_row

	# 擷取近期的氣候資料：
	# 依據 config.ini 內自訂的起始要擷取的資料時段變數，以設定要擷取的資料時段
	def get_recent_climate_data(self, station_id, station_area):
		print('如果 DB 為空，需要擷取 三年前 2015-1-1 ~ 該天的昨天 期間的所有氣候資料')
		yesterday_str = Climate_Common.get_yesterday_date_str()

		daily_start_period = self.recent_climate_data_daily_start_period
		daily_end_period = yesterday_str[:-3]
		hourly_start_period = self.recent_climate_data_hourly_start_period
		hourly_end_period = yesterday_str

		# 取得要擷取的資料時段 (包括天、小時)
		daily_periods = Climate_Common.get_month_periods(daily_start_period, daily_end_period)
		hourly_periods = Climate_Common.get_day_periods(hourly_start_period, hourly_end_period)
		print('daily periods:', daily_periods)
		print('hourly periods:', hourly_periods)

		# 擷取氣候資料
		daily_record_start_period, daily_record_end_period = \
				self.daily_crawler.get_station_climate_data(station_id, daily_periods)
		hourly_record_start_period, hourly_record_end_period = \
				self.hourly_crawler.get_station_climate_data(station_id, hourly_periods)
		print('record daily crawler: {} ~ {}'.format(daily_record_start_period, daily_record_end_period))
		print('record hourly crawler: {} ~ {}\n'.format(hourly_record_start_period, hourly_record_end_period))

		is_daily_record_period = self.is_record_period(daily_record_start_period, daily_record_end_period)
		is_hourly_record_period = self.is_record_period(hourly_record_start_period, hourly_record_end_period)
		print('is_daily_record_period:', is_daily_record_period)
		print('is_hourly_record_period:', is_hourly_record_period)

		if not is_daily_record_period and not is_hourly_record_period:
			return None

		temp_row_dict = {
			'Station_Area': station_area,
			'Reporttime': pd.Timestamp.now(),
			'Daily_Start_Period': np.nan,
			'Daily_End_Period': np.nan,
			'Hourly_Start_Period': np.nan,
			'Hourly_End_Period': np.nan,
			'New_Daily_Start_Period': daily_record_start_period,
			'New_Daily_End_Period': daily_record_end_period,
			'New_Hourly_Start_Period': hourly_record_start_period,
			'New_Hourly_End_Period': hourly_record_end_period
		}
		temp_row = pd.Series(temp_row_dict)
		return temp_row

	def is_record_period(self, record_start_period, record_end_period):
		return record_start_period is not None and record_end_period is not None