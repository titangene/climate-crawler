import pandas as pd
import numpy as np
from urllib.parse import quote

class Climate_Station:
	def __init__(self):
		self.base_url = 'https://e-service.cwb.gov.tw/HistoryDataQuery'
		self.dataFrame = self.read_station_csv_to_df()
		self.all_station_id = self.get_all_station_id()

	# 讀取觀測站 csv
	def read_station_csv_to_df(self):
		climate_station_df = pd.read_csv('data/climate_station.csv')
		climate_station_df.set_index('station_id', inplace=True)
		return climate_station_df

	# 取得所有觀測站 id
	def get_all_station_id(self):
		all_station_id = self.dataFrame.index.values
		return all_station_id

	def encodeURI(self, uri):
		# 包括 '/', '(', ')' 不會做 encode 處理
		return quote(uri, safe='/\(\)')

	# 將觀測站名稱做兩次 encode 處理
	def get_stname(self, station_name):
		return self.encodeURI(self.encodeURI(station_name))

	# 用 觀測站 id 找到對應的 觀測站名稱
	def get_station_name(self, station_id):
		expression = 'station_id == "{}"'.format(station_id)
		station_name = self.dataFrame.query(expression)['station_name'][0]
		return station_name

	# 用 觀測站名稱 找到對應的 觀測站 id
	def get_station_id(self, station_name):
		expression = 'station_name == "{}"'.format(station_name)
		station_id = self.dataFrame.query(expression).index[0]
		return station_id

	# 取得 觀測站 id、名稱、encode
	def get_station(self, station_id=None, station_name=None):
		# 若輸入 觀測站名稱，但未輸入 觀測站 id，會用 觀測站名稱 找 觀測站 id
		if station_name and not station_id:
			station_id = self.get_station_id(station_name)

		# 若輸入 觀測站 id，但未輸入 觀測站名稱，會用 觀測站 id 找 觀測站名稱
		if station_id and not station_name:
			station_name = self.get_station_name(station_id)

		stname = self.get_stname(station_name)
		return station_id, station_name, stname

	# 用 觀測站 id 找到觀測站的所在縣市
	def get_station_location(self, station_id):
		expression = 'station_id == "{}"'.format(station_id)
		station_location = self.dataFrame.query(expression)['location'][0]
		return station_location

	# 取得 觀測站名稱 與 所在縣市
	# e.g. get_station_area('466900')
	# output: '臺北市-淡水'
	def get_station_area(self, station_id):
		station_location = self.get_station_location(station_id)
		station_name = self.get_station_name(station_id)
		station_area = '{}-{}'.format(station_location, station_name)
		return station_area

	# e.g. get_month_periods(start_period='2017-11', end_period='2018-2')
	# output: ['2017-11', '2017-12', '2018-01']
	def get_month_periods(self, start_period, end_period):
		return pd.date_range(start=start_period, end=end_period, freq='MS').strftime('%Y-%m').tolist()

	# e.g. get_day_periods(start_period='2017-12-30', end_period='2018-01-02')
	# output: ['2017-12-30', '2017-12-31', '2018-01-01', '2018-01-02']
	def get_day_periods(self, start_period, end_period):
		return pd.date_range(start=start_period, end=end_period, freq='D').strftime('%Y-%m-%d').tolist()

	# e.g. get_daily_full_url(period='2017-12', station_id='466910')
	def get_daily_full_url(self, period, station_id=None, station_name=None):
		station_id, station_name, stname = self.get_station(station_id, station_name)
		daily_url = 'MonthDataController.do?command=viewMain'
		return '{}/{}&station={}&stname={}&datepicker={}'.format(self.base_url, daily_url, station_id, stname, period)

	# e.g. get_hourly_full_url(period='2017-12-30', station_id='466910')
	def get_hourly_full_url(self, period, station_id=None, station_name=None):
		station_id, station_name, stname = self.get_station(station_id, station_name)
		month_url = 'DayDataController.do?command=viewMain'
		return '{}/{}&station={}&stname={}&datepicker={}'.format(self.base_url, month_url, station_id, stname, period)