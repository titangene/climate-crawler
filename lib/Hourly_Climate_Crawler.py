import logging

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

from lib.config.config import Config
import lib.Climate_Common as Climate_Common
import lib.Request as Request
from lib.csv import csv_process

class Hourly_Climate_Crawler:
	def __init__(self, climate_station, to_mssql):
		self.climate_station = climate_station
		self.to_mssql = to_mssql
		self.db_table_name = Config().get_database_table_name_for_climate_hourly_data()
		self.reserved_columns = ['Temperature', 'Humidity', 'SunShine_hr', 'SunShine_MJ']

	def get_station_climate_data(self, station_id, periods):
		print('--------- hourly climate crawler: Start ---------')
		station_area = self.climate_station.get_station_area(station_id)
		record_start_period = None
		record_end_period = None
		number_of_crawls = 0
		merge_file_name = 'hourly_climate_data.csv'
		file_name = 'hourly_climate/data_{}.csv'.format(station_id)
		# 是否擷取到任何此觀測站的氣候資料
		is_catch_any_data = False

		for period in periods:
			hourly_climate_url = self.climate_station.get_hourly_full_url(period, station_id)
			climate_df = self.catch_climate_data(hourly_climate_url)

			# 如果沒有任何資料就不儲存
			if climate_df is None:
				print(period, station_id, station_area, 'None')
				continue

			climate_df = self.data_preprocess(climate_df, period, station_area)

			if number_of_crawls == 0:
				is_catch_any_data = True
				record_start_period = period
				csv_process.to_csv(climate_df, file_name)

			if number_of_crawls > 0:
				csv_process.to_csv(climate_df, file_name, mode='a', header=False)

			number_of_crawls += 1
			record_end_period = period

			csv_process.to_csv(climate_df, merge_file_name, mode='a', header=False)
			csv_process.to_csv_backup(climate_df, merge_file_name, backup_timestamp, mode='a', header=False)

			self.save_data_to_db(climate_df)
			logging.info('{} {} hourly {}'.format(station_id, station_area, period))

			print(period, station_id, station_area, 'record: {} ~ {}'.format(record_start_period, record_end_period))

		if not is_catch_any_data:
			csv_process.delete_csv(file_name)
			record_start_period = None
			record_end_period = None
		print('--------- hourly climate crawler: End -----------')
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

	def catch_climate_data(self, url):
		req = Request.get(url)
		soup = BeautifulSoup(req.text, 'lxml')

		# 若 <label class="imp"> 此 element 的文字為 '本段時間區間內無觀測資料。' 時，
		# 就代表 CWB 還未將氣候資料上傳至平台
		data_info = soup.find(class_='imp', text='本段時間區間內無觀測資料。')
		if self.CWB_did_not_upload_data(data_info):
			return None

		# 保留欄位德 index
		reserved_columns_index = [0, 3, 5, 12, 13]
		# return: {0: 'Day', 3: 'Temperature', 5: 'Humidity', ... }
		rename_columns = dict(zip(reserved_columns_index, ['Hour'] + self.reserved_columns))

		# iloc[3:, reserved_columns_index] 中的 '3:' 是刪除前 3 列 (index: 0 ~ 2)
		# 將資料內的 '/' 和 'X' 設為 NA
		# 只要 subset 這些欄位全部都 NA 才 drop
		climate_table = soup.find(id='MyTable')
		climate_df = pd.read_html(str(climate_table))[0]\
							.iloc[3:, reserved_columns_index]\
							.rename(columns=rename_columns)\
							.replace('/', np.nan)\
							.replace('X', np.nan)\
							.replace('...', np.nan)

		if climate_df.empty:
			return None

		# 將 Hour 欄位原本的 1 ~ 24 改成 '00' ~ '23'
		climate_df['Hour'] = list(map(self.set_hour_str(), list(climate_df['Hour'])))
		return climate_df

	def CWB_did_not_upload_data(self, data_info):
		return data_info is not None and data_info.text == '本段時間區間內無觀測資料。'

	def save_data_to_db(self, dataSet):
		self.to_mssql.to_sql(dataSet, self.db_table_name, if_exists='append')

	def set_hour_str(self):
		return lambda hour: str(int(hour) - 1).zfill(2)