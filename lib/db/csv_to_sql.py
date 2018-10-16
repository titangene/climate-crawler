from sqlalchemy import create_engine
from pandas.io.sql import SQLDatabase, SQLTable
import pandas as pd
import numpy as np

class csv_to_mssql:
	def __init__(self):
		self.host_ip, self.db_name = self.set_db_config()
		self.user_id = 'SA' # 預設使用者為 SA
		self.pwd = 'taipower@2018'  # 預設使用者密碼

		self.sql_engine = self.create_engine()

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
		self.sql_engine.dispose()

	# 處理 日 氣候資料
	def deal_with_daily_data(self, table_name, csv_name, if_exists='append'):
		dataSet = self.load_csv(csv_name)
		self.to_sql(dataSet, table_name, if_exists)

	# 處理 小時 氣候資料
	def deal_with_hourly_data(self, table_name, csv_name, if_exists='append'):
		dataSet = self.load_csv(csv_name)
		self.to_sql(dataSet, table_name, if_exists)

	def to_sql(self, dataSet, table_name, if_exists, dtype=None, keys=None, sql_table=None):
		result = "This Dataset had been storaged in DB"
		try:
			if keys is not None and sql_table is not None:
				self.to_sql_set_primary_key_and_not_null(dataSet, table_name, self.sql_engine,
						if_exists=if_exists, index=False, keys=keys, sql_table=sql_table)
			if keys is None and sql_table is None:
				dataSet.to_sql(table_name, self.sql_engine, if_exists=if_exists, index=False, dtype=dtype)
		except Exception as e:
			result = e
		else:
			result = '{} ({}): OKAY'.format(table_name, if_exists)
		print(result)

	# DataFrame.to_sql() 自訂版
	# 可自訂 primary key 和 not null
	def to_sql_set_primary_key_and_not_null(self, frame, name, con, keys, sql_table,
			schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None):
		# ref: https://github.com/pandas-dev/pandas/blob/master/pandas/io/sql.py#L437
		if if_exists not in ('fail', 'replace', 'append'):
			raise ValueError("'{0}' is not valid for if_exists".format(if_exists))

		# ref: https://github.com/pandas-dev/pandas/blob/master/pandas/io/sql.py#L508
		pandas_sql = SQLDatabase(con, schema=schema)

		if isinstance(frame, pd.Series):
			frame = frame.to_frame()
		elif not isinstance(frame, pd.DataFrame):
			raise NotImplementedError("'frame' argument should be either a Series or a DataFrame")

		if dtype is not None:
			from sqlalchemy.types import to_instance, TypeEngine
			for col, my_type in dtype.items():
				if not isinstance(to_instance(my_type), TypeEngine):
					raise ValueError('The type of {} is not a SQLAlchemy type '.format(col))

		table = SQLTable(name, pandas_sql, frame=frame, index=index, if_exists=if_exists, index_label=index_label, schema=schema, keys=keys, dtype=dtype)
		table.table = sql_table
		table.create()
		table.insert(chunksize)

	# 處理 日 和 小時 氣候資料
	def deal_with_daily_and_hourly_data(self):
		self.deal_with_daily_data(table_name='Daily_Climate_data', csv_name='daily_climate_data.csv')
		self.deal_with_hourly_data(table_name='Hourly_Climate_data', csv_name='hourly_climate_data.csv')