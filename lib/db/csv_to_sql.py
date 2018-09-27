# -*- coding: utf-8 -*-

import sqlalchemy as sqlc
import pandas as pd

class csv_to_mssql:
	def __init__(self, host_ip, DB_name):
		self.host_ip = host_ip
		self.DB_name = DB_name
		self.user_id = 'SA' # 預設使用者為 SA
		self.pwd = 'taipower@2018'  # 預設使用者密碼

		self.engine = self.create_engine()

	def set_sql_url(self):
		return 'mssql+pyodbc://{}:{}@{}/{}?driver=SQL+Server'.format(self.user_id, self.pwd, self.host_ip, self.DB_name)

	def create_engine(self):
		url = self.set_sql_url()
		return sqlc.create_engine(url, connect_args={'charset':'utf-8'})

	def load_csv(self, csv_name):
		doc_name = 'data/'+ csv_name
		dataSet = pd.read_csv(doc_name)
		return dataSet

	# 處理日氣候資料
	def deal_with_dilay_data(self, table_name, csv_name, if_exists='append'):
		dataSet = self.load_csv(csv_name)
		dataSet.drop('Month', axis=1, inplace=True)
		result = "This Dataset had been storaged in DB"
		try:
			dataSet.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
		except Exception as e:
			result = e
		else:
			result = '{} ({}): OKAY'.format(table_name, if_exists)
		return result

	# 處理小時氣候資料
	def deal_with_hourly_data(self, table_name, csv_name, if_exists='append'):
		dataSet = self.load_csv(csv_name)
		dataSet.drop('Day', axis=1, inplace=True)
		result = "This Dataset had been storaged in DB"
		try:
			dataSet.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
		except Exception as e:
			result = e
		else:
			result = '{} ({}): OKAY'.format(table_name, if_exists)
		return result