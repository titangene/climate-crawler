from urllib.parse import quote
import pandas as pd
import numpy as np

from lib.Climate_Station import Climate_Station

class Daily_Climate_Crawler:
	def __init__(self):
		self.climate_station = Climate_Station()
		self.all_station_id = self.climate_station.all_station_id

	def obtain_daily_data(self, start_period, end_period):
		return_df = pd.DataFrame()
		periods = self.climate_station.get_month_periods(start_period, end_period)

		for period in periods:
			for station_id in self.all_station_id:
				daily_climate_url = self.climate_station.get_daily_full_url(period, station_id)

				temp_df = self.catach_climate_data(daily_climate_url)

				# 如果沒有任何資料就不儲存
				# if temp_df.dropna().empty:
				# 	break

				station_area = self.climate_station.get_station_area(station_id)

				temp_df['Month'] = period
				temp_df['Reporttime'] = period + '-' + temp_df['Day']
				temp_df.drop(['Day'], axis=1, inplace=True)
				temp_df['Area'] = station_area
				temp_df['UUID'] = temp_df['Reporttime'] + '_' + temp_df['Area']

				# 將欄位重新排序成 DB 的欄位順序
				new_index = ['UUID', 'Area', 'Temperature', 'Max_T', 'Min_T', 'Humidity', 'Month', 'Reporttime']
				temp_df = temp_df.reindex(new_index, axis=1)

				return_df = pd.concat([return_df, temp_df], ignore_index=True)
				print(period, station_id, station_area)

			return_df.to_csv('data/daily_climate_data.csv', index=False, encoding='utf8')
		return return_df

	def catach_climate_data(self, url):
		climate_table = pd.read_html(url, encoding=str)
		climate_df = climate_table[1]
		climate_df.rename(columns={
				climate_df.columns[0]: 'Day',
				climate_df.columns[7]: 'Temperature',
				climate_df.columns[8]: 'Max_T',
				climate_df.columns[10]: 'Min_T',
				climate_df.columns[13]: 'Humidity'
		}, inplace=True)
		# 刪除列
		climate_df.drop([0, 1, 2], inplace=True)
		# 刪除排
		df_drop_index = list(range(1, 7)) + [9, 11, 12] + list(range(14, 35))
		climate_df.drop(df_drop_index, axis=1, inplace=True)
		return climate_df