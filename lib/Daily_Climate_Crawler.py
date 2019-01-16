import logging

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

from lib.config.config import Config
import lib.Climate_Common as Climate_Common
import lib.Request as Request
from lib.csv import csv_process

class Daily_Climate_Crawler:
	def __init__(self, climate_station, to_mssql):
		self.climate_station = climate_station
		self.to_mssql = to_mssql
		self.db_table_name = Config().get_database_table_name_for_climate_daily_data()
		self.reserved_columns = ['Temperature', 'Max_T', 'Min_T', 'Humidity', 'SunShine_hr', 'SunShine_MJ']

	def get_station_climate_data(self, station_id, periods, filter_period=None):
		print('---------- daily climate crawler: Start ---------')
		station_area = self.climate_station.get_station_area(station_id)
		record_start_period = None
		record_end_period = None
		number_of_crawls = 0
		file_name = 'daily_climate/data_{}.csv'.format(station_id)
		# 是否擷取到任何此觀測站的氣候資料
		is_catch_any_data = False

		for period in periods:
			daily_climate_url = self.climate_station.get_daily_full_url(period, station_id)
			climate_df = self.catch_climate_data(daily_climate_url, period)

			# 如果沒有任何資料就不儲存
			if climate_df is None:
				print(period, station_id, station_area, 'None')
				continue

			climate_df = self.data_preprocess(climate_df, period, station_area, filter_period)

			# 過濾資料後，如果沒有任何資料就不儲存
			if climate_df.empty:
				print(period, station_id, station_area, 'filter None')
				continue

			# 記錄爬蟲 log
			if number_of_crawls == 0:
				is_catch_any_data = True
				record_start_period = self.record_crawler_log_start_period(climate_df)
				csv_process.to_csv(climate_df, file_name)

			if number_of_crawls > 0:
				csv_process.to_csv(climate_df, file_name, mode='a', header=False)

			number_of_crawls += 1
			record_end_period = self.record_crawler_log_end_period(climate_df)

			self.save_data_to_db(climate_df)
			logging.info('{} {} daily {}'.format(station_id, station_area, period))

			print(period, station_id, station_area, 'record: {} ~ {}'.format(record_start_period, record_end_period))

		if not is_catch_any_data:
			csv_process.delete_csv(file_name)
			record_start_period = None
			record_end_period = None
		print('---------- daily climate crawler: End -----------')
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
	def filter_out_duplicate_data(self, dataSet, filter_period):
		# 只留需要的日期區間
		period_month_end = (pd.Timestamp(filter_period) + pd.offsets.MonthEnd(0)).strftime('%Y-%m-%d')
		print('filter: {} ~ {}'.format(filter_period, period_month_end))
		maskTime = dataSet['Reporttime'].between(filter_period, period_month_end)
		return dataSet[maskTime]

	# 記錄爬蟲 log 之起始時間 (第一筆的 Reporttime)
	def record_crawler_log_start_period(self, dataSet):
		record_start_period = dataSet.iloc[0]['Reporttime']
		return record_start_period

	# 記錄爬蟲 log 之終止時間 (最後一筆的 Reporttime)
	def record_crawler_log_end_period(self, dataSet):
		tmp_dataSet = dataSet.dropna(subset=self.reserved_columns, how='all')
		record_end_period = tmp_dataSet.iloc[-1]['Reporttime']
		return record_end_period

	def save_data_to_db(self, dataSet):
		self.to_mssql.to_sql(dataSet, self.db_table_name, if_exists='append')

	def catch_climate_data(self, url, period):
		req = Request.get(url)
		soup = BeautifulSoup(req.text, 'lxml')

		# 若 <label class="imp"> 此 element 的文字為 '本段時間區間內無觀測資料。' 時，
		# 就代表 CWB 還未將氣候資料上傳至平台
		data_info = soup.find(class_='imp', text='本段時間區間內無觀測資料。')
		if self.CWB_did_not_upload_data(data_info):
			return None

		climate_table = soup.find(id='MyTable')
		climate_df = pd.read_html(str(climate_table))[0]

		# 保留欄位德 index
		reserved_columns_index = [0, 7, 8, 10, 13, 27, 29]
		# return: {0: 'Day', 7: 'Temperature', 8: 'Max_T', ... }
		rename_columns = dict(zip(reserved_columns_index, ['Day'] + self.reserved_columns))

		# 原始資料的所有欄位名稱，不包含日期那欄
		source_date_column_name = 1
		source_column_without_date = climate_df.columns[source_date_column_name:]

		# 將資料內的 '/', X', '...' 設為 NA
		climate_df = climate_df.replace('/', np.nan)\
							   .replace('X', np.nan)\
							   .replace('...', np.nan)

		yesterday = Climate_Common.get_yesterday_date_str()
		if self.is_same_year_month(period, yesterday):
			# e.g. 將 '2018-10-05' 變成 '05'，只取日
			yesterday_day = yesterday[-2:]
			# 原始資料的欄位名稱最後一列的 index
			source_column_name_end_index = 2
			# 昨天的資料列 index
			yesterday_index = int(yesterday_day) + source_column_name_end_index
			# 第一筆的資料列 index
			source_first_data_row_index = 3
			# 過濾掉未來時段的資料列
			climate_df = climate_df.loc[source_first_data_row_index: yesterday_index]

		# 1. 只要 subset 這些欄位全部都 NA 才 drop 該列
		# 2. 只保留 reserved_columns_index 這些欄位
		# 3. 欄位重新命名
		climate_df = climate_df.dropna(subset=source_column_without_date, how='all')\
							   [reserved_columns_index]\
							   .rename(columns=rename_columns)

		if climate_df.empty:
			return None

		return climate_df

	def CWB_did_not_upload_data(self, data_info):
		return data_info is not None and data_info.text == '本段時間區間內無觀測資料。'