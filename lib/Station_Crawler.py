import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import json
import requests
import re

class Station_Crawler:
	def start(self):
		url = 'https://e-service.cwb.gov.tw/HistoryDataQuery/QueryDataController.do?command=viewMain'
		req = requests.get(url)
		soup = BeautifulSoup(req.text, 'html.parser')

		js_str = soup.find_all("script")[-1].string
		js_str = self.parse_js(js_str)

		json_data = self.parse_json(js_str)
		climate_station_dict = self.data_to_dict(json_data)
		self.climate_station_df = self.data_to_dataFrame(climate_station_dict)
		self.save_data_to_csv()

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

	def data_to_dict(self, data):
		climate_station_dict = {'station_id': [], 'station_name': [], 'location': []}

		for key, value in data.items():
			cancellation_station = value[3]
			# e.g. '467780': ['七股', 'QIGU', '臺南市', '2']
			# 資料內的最後一個參數 "2" 代表此觀測站已撤銷，因此不紀錄此觀測站
			if (cancellation_station == '2'):
				continue
			station_id = key
			station_name = value[0]
			location = value[2]
			climate_station_dict['station_id'].append(station_id)
			climate_station_dict['station_name'].append(station_name)
			climate_station_dict['location'].append(location)
		return climate_station_dict

	def data_to_dataFrame(self, climate_station_dict):
		climate_station_df = pd.DataFrame(climate_station_dict)
		return climate_station_df

	def save_data_to_csv(self):
		file_path = 'data/climate_station.csv'
		self.climate_station_df.to_csv(file_path, encoding='utf-8', index=False)
		print('save: {}'.format(file_path))