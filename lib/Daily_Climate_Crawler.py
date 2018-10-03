from urllib.parse import quote
import pandas as pd
import numpy as np

from lib.Climate_Station import Climate_Station

class Daily_Climate_Crawler:
	def __init__(self):
		self.climate_station = Climate_Station()
		self.all_station_id = self.climate_station.all_station_id
		self.reserved_columns = ['Temperature', 'Max_T', 'Min_T', 'Humidity', 'SunShine_hr', 'SunShine_MJ']

	def obtain_daily_data(self, start_period, end_period):
		print('---------- daily climate crawler: Start ---------')
		return_df = pd.DataFrame()
		periods = self.climate_station.get_month_periods(start_period, end_period)

		for period in periods:
			for station_id in self.all_station_id:
				daily_climate_url = self.climate_station.get_daily_full_url(period, station_id)
				temp_df = self.catch_climate_data(daily_climate_url)
				# 如果沒有任何資料就不儲存
				if temp_df is None:
					break

				station_area = self.climate_station.get_station_area(station_id)
				temp_df['Reporttime'] = period + '-' + temp_df['Day']
				temp_df['Area'] = station_area
				temp_df['UUID'] = temp_df['Reporttime'] + '_' + temp_df['Area']
				temp_df.drop(['Day'], axis=1, inplace=True)

				# 將欄位重新排序成 DB 的欄位順序
				new_index = ['UUID', 'Area'] + self.reserved_columns + ['Reporttime']
				temp_df = temp_df.reindex(new_index, axis=1)

				return_df = pd.concat([return_df, temp_df], ignore_index=True)
				print(period, station_id, station_area)

			return_df.to_csv('data/daily_climate_data.csv', index=False, encoding='utf8')
		print('---------- daily climate crawler: End ---------')
		return return_df

	def catch_climate_data(self, url):
		# 保留欄位德 index
		reserved_columns_index = [0, 7, 8, 10, 13, 27, 29]
		# return: {0: 'Day', 7: 'Temperature', 8: 'Max_T', ... }
		rename_columns = dict(zip(reserved_columns_index, ['Day'] + self.reserved_columns))

		# iloc[3:, reserved_columns_index] 中的 '3:' 是刪除前 3 列 (index: 0 ~ 2)
		# 將資料內的 '/' 設為 NA
		# 只要 subset 這些欄位全部都 NA 才 drop
		climate_table = pd.read_html(url, attrs={'id': 'MyTable'}, encoding=str)[0]
		climate_df = climate_table.iloc[3:, reserved_columns_index]\
								  .rename(columns=rename_columns)\
								  .replace('/', np.nan)\
								  .dropna(subset=self.reserved_columns, how='all')

		return climate_df if not climate_df.empty else None