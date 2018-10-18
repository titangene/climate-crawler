from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np

import lib.Climate_Common as Climate_Common
from lib.csv import csv_process

class Hourly_Climate_Crawler:
	def __init__(self, climate_station):
		self.climate_station = climate_station
		self.reserved_columns = ['Temperature', 'Humidity', 'SunShine_hr', 'SunShine_MJ']

	def get_station_climate_data(self, station_id, periods):
		print('--------- hourly climate crawler: Start ---------')
		station_area = self.climate_station.get_station_area(station_id)
		climate_df = pd.DataFrame()
		record_start_period = None
		record_end_period = None

		for period_idx, period in enumerate(periods):
			hourly_climate_url = self.climate_station.get_hourly_full_url(period, station_id)
			temp_df = self.catch_climate_data(hourly_climate_url)

			# 如果沒有任何資料就不儲存
			if temp_df is None:
				print(period, station_id, station_area, 'None')
				break

			temp_df = self.data_preprocess(temp_df, period, station_area)

			# 記錄爬蟲 log (最後一筆的 Reporttime)
			if self.is_twenty_three_oclock(temp_df):
				if period_idx == 0:
					record_start_period = period

				record_end_period = period
			else:
				break

			climate_df = pd.concat([climate_df, temp_df], ignore_index=True)
			print(period, station_id, station_area, 'record: {} ~ {}'.format(record_start_period, record_end_period))
			# print(temp_df.tail(2))

		if not climate_df.empty:
			csv_process.to_csv(climate_df, 'hourly_climate/data_{}.csv'.format(station_id))
		print('--------- hourly climate crawler: End ---------')
		return record_start_period, record_end_period

	def data_preprocess(self, df, period, station_area):
		df['Reporttime'] = period + ' ' + df['Hour'] + ':00'
		df['Area'] = station_area
		df['UUID'] = period + '_' + df['Hour'] + '_' + df['Area']

		# 將欄位重新排序成 DB 的欄位順序
		new_index = ['UUID', 'Area'] + self.reserved_columns + ['Reporttime']
		df = df.drop(['Hour'], axis=1)\
			   .reindex(new_index, axis=1)
		return df

	# 是否有 23:00 這筆資料
	def is_twenty_three_oclock(self, df):
		record_period = df.iloc[-1]['Reporttime']
		return record_period.endswith('23:00')

	def catch_climate_data(self, url):
		req = requests.get(url)
		soup = BeautifulSoup(req.text, 'lxml')

		data_info = soup.find(class_='imp').text
		if data_info == '本段時間區間內無觀測資料。':
			return None
		else:
			# 保留欄位德 index
			reserved_columns_index = [0, 3, 5, 12, 13]
			# return: {0: 'Day', 3: 'Temperature', 5: 'Humidity', ... }
			rename_columns = dict(zip(reserved_columns_index, ['Hour'] + self.reserved_columns))

			# iloc[3:, reserved_columns_index] 中的 '3:' 是刪除前 3 列 (index: 0 ~ 2)
			# 將資料內的 '/' 設為 NA
			# 只要 subset 這些欄位全部都 NA 才 drop
			climate_table = soup.find(id='MyTable')
			climate_df = pd.read_html(str(climate_table))[0]\
						   .iloc[3:, reserved_columns_index]\
						   .rename(columns=rename_columns)\
						   .replace('/', np.nan)\
						   .dropna(subset=self.reserved_columns, how='all')

			if climate_df.empty:
				return None

			# 將 Hour 欄位原本的 1 ~ 24 改成 '00' ~ '23'
			climate_df['Hour'] = list(map(lambda hour: str(hour).zfill(2), range(0, 24)))
			return climate_df