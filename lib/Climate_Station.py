import pandas as pd
import numpy as np
from urllib.parse import quote

class Climate_Station:
	def __init__(self):
		self.base_url = 'https://e-service.cwb.gov.tw/HistoryDataQuery'
		self.hourly_url = 'DayDataController.do?command=viewMain'
		self.daily_url = 'MonthDataController.do?command=viewMain'
		self.dataFrame = self.read_station_csv_to_df()
		self.all_station_id = self.get_all_station_id()

		self.dataFrame['stname'] = self.dataFrame['station_name'].apply(lambda name: self.set_stname(name))

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
		return quote(uri, safe=r'/\(\)')

	# 將觀測站名稱做兩次 encode 處理
	def set_stname(self, station_name):
		return self.encodeURI(self.encodeURI(station_name))

	# 用 觀測站 id 找到對應的 觀測站名稱
	def get_station_name(self, station_id):
		expression = 'station_id == "{}"'.format(station_id)
		station_name = self.dataFrame.query(expression)['station_name'][0]
		return station_name

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

	def get_full_url(self, sub_url, station_id, period):
		stname = self.dataFrame.loc[station_id]['stname']
		return '{}/{}&station={}&stname={}&datepicker={}'.format(self.base_url, sub_url, station_id, stname, period)

	# e.g. get_daily_full_url(period='2017-12', station_id='466910')
	def get_daily_full_url(self, period, station_id):
		return self.get_full_url(self.daily_url, station_id, period)

	# e.g. get_hourly_full_url(period='2017-12-30', station_id='466910')
	def get_hourly_full_url(self, period, station_id):
		return self.get_full_url(self.hourly_url, station_id, period)