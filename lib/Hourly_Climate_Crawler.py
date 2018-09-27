from urllib.parse import quote
import pandas as pd
import numpy as np

from lib.Climate_Station import Climate_Station

class Hourly_Climate_Crawler:
	def __init__(self):
		self.climate_station = Climate_Station()
		self.all_station_id = self.climate_station.all_station_id

	def obtain_hourly_data(self, start_period, end_period):
		print('--------- hourly climate crawler: Start ---------')
		return_df = pd.DataFrame()
		periods = self.climate_station.get_day_periods(start_period, end_period)

		for period in periods:
			for station_id in self.all_station_id:
				daily_climate_url = self.climate_station.get_hourly_full_url(period, station_id)
				temp_df = self.catach_climate_data(daily_climate_url)
				# 如果沒有任何資料就不儲存
				if temp_df is None:
					break

				station_area = self.climate_station.get_station_area(station_id)
				temp_df['Day'] = period
				temp_df['Reporttime'] = period + ' ' + temp_df['Hour'] + ':00'
				temp_df['Area'] = station_area
				temp_df['UUID'] = period + '_' + temp_df['Hour'] + '_' + temp_df['Area']
				temp_df.drop(['Hour'], axis=1, inplace=True)

				# 將欄位重新排序成 DB 的欄位順序
				new_index = ['UUID', 'Area', 'Temperature', 'Humidity', 'Day', 'Reporttime']
				temp_df = temp_df.reindex(new_index, axis=1)

				return_df = pd.concat([return_df, temp_df], ignore_index=True)
				print(period, station_id, station_area)

			return_df.to_csv('data/hourly_climate_data.csv', index=False, encoding='utf8')
		print('--------- hourly climate crawler: End ---------')
		return return_df

	def catach_climate_data(self, url):
		climate_table = pd.read_html(url, encoding=str)
		climate_df = climate_table[1]
		climate_df.rename(columns={
			climate_df.columns[0]: 'Hour',
			climate_df.columns[3]: 'Temperature',
			climate_df.columns[5]: 'Humidity'
		}, inplace=True)
		# 刪除列
		climate_df.drop([0, 1, 2], inplace=True)
		# 刪除排
		df_drop_index = [1, 2 ,4] + list(range(6, 17))
		climate_df.drop(df_drop_index, axis=1, inplace=True)
		# 將 Hour 欄位原本的 1 ~ 24 改成 '00' ~ '23'
		climate_df['Hour'] = list(map(lambda hour: str(hour).zfill(2), range(0, 24)))
		# 將 '/' 設為 NA
		climate_df.replace('/', np.nan, inplace=True)
		# 只要 subset 這些欄位全部都 NA 就 drop
		climate_df.dropna(subset=['Temperature', 'Humidity'], how='all', inplace=True)
		return climate_df if not climate_df.empty else None