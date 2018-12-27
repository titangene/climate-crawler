import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import json
import re

from lib.csv import csv_process
import lib.Request as Request

class Station_Crawler:
	def start(self):
		url = 'https://e-service.cwb.gov.tw/HistoryDataQuery/QueryDataController.do?command=viewMain'
		req = Request.get(url)
		soup = BeautifulSoup(req.text, 'html.parser')

		js_str = soup.find_all("script")[-1].string
		js_str = self.parse_js(js_str)

		json_data = self.parse_json(js_str)
		station_df = self.json_to_dataFrame(json_data)

		self.save_df_to_csv(station_df)

	def parse_js(self, js_str):
		# 去除多餘的換行和 tab
		js_str = re.sub('[\t\r\n]', '', js_str)
		# 將一些觀測站名稱的 `&#039` 換成 `'` (單引號)
		js_str = re.sub('&#039', '\'', js_str)
		return js_str

	def parse_json(self, js_str):
		regex_match = re.search(r'var stList = (\{.*?\});', js_str)
		json_str = regex_match.group(1)
		# 去除多餘的 space
		json_str = re.sub(' ', '', json_str)
		json_data = json.loads(json_str)
		return json_data

	# 資料來源 e.g. {"466880":["板橋", "BANQIAO", "新北市", "1"], "467780": ["七股", "QIGU", "臺南市", "2"]}
	# 資料內的最後一個參數 "2" 代表此觀測站已撤銷 (cancellation_station)，因此不紀錄此觀測站
	# output columns: station_id, station_name, location
	def json_to_dataFrame(self, json_data):
		df_columns = ['station_name', 'station_name_eng', 'location', 'cancellation_station']
		# 去除 station_id 開頭為 'C1' 的觀測站，這些觀測站只有提供降水量資料
		# ref: http://e-service.cwb.gov.tw/HistoryDataQuery/downloads/Readme.pdf
		station_df = pd.DataFrame.from_dict(json_data, orient='index', columns=df_columns)\
								 .filter(regex='^([^C].|C[^1]).{4}', axis=0)\
								 .reset_index()\
								 .rename(columns={'index': 'station_id'})\
								 .query('cancellation_station == "1"')\
								 .drop(['station_name_eng', 'cancellation_station'], axis=1)
		return station_df

	def save_df_to_csv(self, station_df):
		csv_process.to_csv(station_df, 'climate_station.csv', backup=True)
		print('update: climate station')