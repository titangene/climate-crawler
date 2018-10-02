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