from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np

from lib.Climate_Station import Climate_Station

class Hourly_Climate_Crawler:
	def __init__(self):
		self.climate_station = Climate_Station()
		self.all_station_id = self.climate_station.all_station_id
		self.reserved_columns = ['Temperature', 'Humidity', 'SunShine_hr', 'SunShine_MJ']

	def obtain_hourly_data(self, start_period, end_period):
		print('--------- hourly climate crawler: Start ---------')
		return_df = pd.DataFrame()
		# e.g. get_day_periods(start_period='2017-12-30', end_period='2018-01-02')
		# output: ['2017-12-30', '2017-12-31', '2018-01-01', '2018-01-02']
		periods = self.climate_station.get_day_periods(start_period, end_period)

		for period in periods:
			for station_id in self.all_station_id:
				hourly_climate_url = self.climate_station.get_hourly_full_url(period, station_id)
				temp_df = self.catch_climate_data(hourly_climate_url)
				# 如果沒有任何資料就不儲存
				if temp_df is None:
					break

				station_area = self.climate_station.get_station_area(station_id)
				temp_df['Reporttime'] = period + ' ' + temp_df['Hour'] + ':00'
				temp_df['Area'] = station_area
				temp_df['UUID'] = period + '_' + temp_df['Hour'] + '_' + temp_df['Area']
				temp_df.drop(['Hour'], axis=1, inplace=True)

				# 將欄位重新排序成 DB 的欄位順序
				new_index = ['UUID', 'Area'] + self.reserved_columns + ['Reporttime']
				temp_df = temp_df.reindex(new_index, axis=1)

				return_df = pd.concat([return_df, temp_df], ignore_index=True)
				print(period, station_id, station_area)

			return_df.to_csv('data/hourly_climate_data.csv', index=False, encoding='utf8')
		print('--------- hourly climate crawler: End ---------')
		return return_df

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

			# 將 Hour 欄位原本的 1 ~ 24 改成 '00' ~ '23'
			climate_df['Hour'] = list(map(lambda hour: str(hour).zfill(2), range(0, 24)))
			return climate_df
