from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np

import lib.Climate_Common as Climate_Common
from lib.csv import csv_process

class Daily_Climate_Crawler:
	def __init__(self, climate_station):
		self.climate_station = climate_station
		self.reserved_columns = ['Temperature', 'Max_T', 'Min_T', 'Humidity', 'SunShine_hr', 'SunShine_MJ']

	def get_station_climate_data(self, station_id, periods, filter_period=None):
		station_area = self.climate_station.get_station_area(station_id)
		climate_df = pd.DataFrame()
		record_start_period = None
		record_end_period = None

		for period_idx, period in enumerate(periods):
			daily_climate_url = self.climate_station.get_daily_full_url(period, station_id)
			temp_df = self.catch_climate_data(daily_climate_url)

			# 如果沒有任何資料就不儲存
			if temp_df is None:
				break

			temp_df = self.data_preprocess(temp_df, period, station_area, filter_period)

			# 過濾資料後，如果沒有任何資料就不儲存
			if temp_df.empty:
				break

			# 記錄爬蟲 log
			if period_idx == 0:
				record_start_period = self.record_crawler_log_start_period(temp_df)

			record_end_period = self.record_crawler_log_end_period(temp_df)

			climate_df = pd.concat([climate_df, temp_df], ignore_index=True)

		file_name = 'daily_climate/data_{}.csv'.format(station_id)
		if climate_df.empty:
			csv_process.delete_csv(file_name)
		else:
			csv_process.to_csv(climate_df, file_name)
		return record_start_period, record_end_period

	def data_preprocess(self, df, period, station_area, filter_period):
		df['Reporttime'] = period + '-' + df['Day']
		df['Area'] = station_area
		df['UUID'] = df['Reporttime'] + '_' + df['Area']

		# 將欄位重新排序成 DB 的欄位順序
		new_index = ['UUID', 'Area'] + self.reserved_columns + ['Reporttime']
		df = df.drop(['Day'], axis=1)\
			   .reindex(new_index, axis=1)

		if self.is_same_year_month(period, filter_period):
			df = self.filter_out_duplicate_data(df, filter_period)

		return df

	# period 是否與 filter_period 同年同月份
	# filter_period 就是 start_period
	def is_same_year_month(self, period, filter_period):
		return filter_period and period == filter_period[:-3]

	# period 是否與 filter_period 同年同月份
	# filter_period 就是 start_period
	def filter_out_duplicate_data(self, df, filter_period):
		# 只留需要的日期區間
		period_month_end = (pd.Timestamp(filter_period) + pd.offsets.MonthEnd(0)).strftime('%Y-%m-%d')
		maskTime = df['Reporttime'].between(filter_period, period_month_end)
		temp_df = df[maskTime]
		return temp_df

	# 記錄爬蟲 log 之起始時間 (第一筆的 Reporttime)
	def record_crawler_log_start_period(self, df):
		record_start_period = df.iloc[0]['Reporttime']
		return record_start_period

	# 記錄爬蟲 log 之終止時間 (最後一筆的 Reporttime)
	def record_crawler_log_end_period(self, df):
		record_end_period = df.iloc[-1]['Reporttime']
		return record_end_period

	def catch_climate_data(self, url):
		req = requests.get(url)
		soup = BeautifulSoup(req.text, 'lxml')

		data_info = soup.find(class_='imp').text
		if data_info == '本段時間區間內無觀測資料。':
			return None
		else:
			# 保留欄位德 index
			reserved_columns_index = [0, 7, 8, 10, 13, 27, 29]
			# return: {0: 'Day', 7: 'Temperature', 8: 'Max_T', ... }
			rename_columns = dict(zip(reserved_columns_index, ['Day'] + self.reserved_columns))

			# iloc[3:, reserved_columns_index] 中的 '3:' 是刪除前 3 列 (index: 0 ~ 2)
			# 將資料內的 '/' 和 'X' 設為 NA
			# 只要 subset 這些欄位全部都 NA 才 drop
			climate_table = soup.find(id='MyTable')
			climate_df = pd.read_html(str(climate_table))[0]\
						   .iloc[3:, reserved_columns_index]\
						   .rename(columns=rename_columns)\
						   .replace('/', np.nan)\
						   .replace('X', np.nan) \
							 .replace('...', np.nan)\
						   .dropna(subset=self.reserved_columns, how='all')

			if climate_df.empty:
				return None

			return climate_df