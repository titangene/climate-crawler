from sqlalchemy import create_engine
import pandas as pd
import numpy as np

class csv_to_mssql:
	def __init__(self):
		self.host_ip, self.db_name = self.set_db_config()
		self.user_id = 'SA' # 預設使用者為 SA
		self.pwd = 'taipower@2018'  # 預設使用者密碼

		self.engine = self.create_engine()

	# return: host_ip, user_id
	# e.g. host_ip = '192.168.191.130:1433', user_id = 'Test_DB'
	def set_db_config(self):
		db_config_file_path = 'lib/db/db_config.csv'
		db_config = pd.read_csv(db_config_file_path)
		return list(db_config.iloc[0])

	def set_sql_url(self):
		return 'mssql+pyodbc://{}:{}@{}/{}?driver=SQL+Server'.format(self.user_id, self.pwd, self.host_ip, self.db_name)

	def create_engine(self):
		url = self.set_sql_url()
		return create_engine(url)

	def load_csv(self, csv_name):
		doc_name = 'data/'+ csv_name
		dataSet = pd.read_csv(doc_name)
		return dataSet

	def disconnect(self):
		self.engine.dispose()

	# 處理 日 氣候資料
	def deal_with_daily_data(self, table_name, csv_name, if_exists='append'):
		dataSet = self.load_csv(csv_name)
		result = self.to_sql(dataSet, table_name, if_exists)
		return result

	# 處理 小時 氣候資料
	def deal_with_hourly_data(self, table_name, csv_name, if_exists='append'):
		dataSet = self.load_csv(csv_name)
		result = self.to_sql(dataSet, table_name, if_exists)
		return result

	def to_sql(self, dataSet, table_name, if_exists):
		result = "This Dataset had been storaged in DB"
		try:
			dataSet.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
		except Exception as e:
			result = e
		else:
			result = '{} ({}): OKAY'.format(table_name, if_exists)
		return result

	# 處理 日 和 小時 氣候資料
	def deal_with_daily_and_hourly_data(self):
		daily_climate_to_sql = self.deal_with_daily_data(table_name='Daily_Climate_data', csv_name='daily_climate_data.csv')
		print(daily_climate_to_sql)

		hourly_climate_to_sql = self.deal_with_hourly_data(table_name='Hourly_Climate_data', csv_name='hourly_climate_data.csv')
		print(hourly_climate_to_sql)