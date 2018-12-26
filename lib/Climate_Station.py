import pandas as pd
import numpy as np
from urllib.parse import quote

from lib.config.config import Config
from lib.csv.csv_process import load_csv

class Climate_Station:
	def __init__(self):
		self.base_url = 'https://e-service.cwb.gov.tw/HistoryDataQuery/'
		self.hourly_url = self.base_url + 'DayDataController.do?command=viewMain'
		self.daily_url = self.base_url + 'MonthDataController.do?command=viewMain'

		self.station_df = self.read_station_csv_to_df()
		self.station_df = self.get_filter_station_df(self.station_df)
		self.station_id_list = self.get_station_id_list()
		self.station_df = self.init_columns(self.station_df)

	def init_columns(self, station_df):
		station_df['stname'] = station_df['station_name'].apply(lambda name: self.set_stname(name))
		station_df['station_area'] = station_df.index.map(lambda id: self.set_station_area(id))
		return station_df

	def read_station_csv_to_df(self):
		climate_station_df = load_csv('climate_station.csv').set_index('station_id')
		return climate_station_df

	def get_filter_station_df(self, station_df):
		crawler_cities = Config().get_crawler_cities()
		if crawler_cities == 'all':
			return station_df
		else:
			return station_df[station_df['location'].isin(crawler_cities)]

	# 取得所有觀測站 id
	def get_station_id_list(self):
		station_id_list = self.station_df.index.values
		return station_id_list

	def encodeURI(self, uri):
		# 包括 '/', '(', ')' 不會做 encode 處理
		return quote(uri, safe=r'/\(\)')

	# 將觀測站名稱做兩次 encode 處理
	def set_stname(self, station_name):
		return self.encodeURI(self.encodeURI(station_name))

	# 設定 觀測站名稱 與 所在縣市
	# e.g. set_station_area('466900')
	# output: '臺北市-淡水'
	def set_station_area(self, station_id):
		station_location = self.get_station_location(station_id)
		station_name = self.get_station_name(station_id)
		station_area = '{}-{}'.format(station_location, station_name)
		return station_area

	# 取得 觀測站名稱 與 所在縣市
	# e.g. get_station_area('466900')
	# output: '臺北市-淡水'
	def get_station_area(self, station_id):
		station_area = self.station_df.loc[station_id]['station_area']
		return station_area

	# 用 觀測站 id 找到對應的 觀測站名稱
	def get_station_name(self, station_id):
		station_name = self.station_df.loc[station_id]['station_name']
		return station_name

	# 用 觀測站 id 找到觀測站的所在縣市
	def get_station_location(self, station_id):
		station_location = self.station_df.loc[station_id]['location']
		return station_location

	def get_full_url(self, url, station_id, period):
		stname = self.station_df.loc[station_id]['stname']
		full_url = '{}&station={}&stname={}&datepicker={}'.format(url, station_id, stname, period)
		return full_url

	# e.g. get_daily_full_url(period='2017-12', station_id='466910')
	def get_daily_full_url(self, period, station_id):
		daily_full_url = self.get_full_url(self.daily_url, station_id, period)
		return daily_full_url

	# e.g. get_hourly_full_url(period='2017-12-30', station_id='466910')
	def get_hourly_full_url(self, period, station_id):
		hourly_full_url = self.get_full_url(self.hourly_url, station_id, period)
		return hourly_full_url