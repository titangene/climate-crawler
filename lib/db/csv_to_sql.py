from sqlalchemy import create_engine, Table, Column, DateTime, CHAR, NCHAR
from sqlalchemy.schema import MetaData
from pandas.io.sql import SQLDatabase, SQLTable
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
		self.to_sql(dataSet, table_name, if_exists)

	# 處理 小時 氣候資料
	def deal_with_hourly_data(self, table_name, csv_name, if_exists='append'):
		dataSet = self.load_csv(csv_name)
		self.to_sql(dataSet, table_name, if_exists)

	def to_sql(self, dataSet, table_name, if_exists, dtype=None, keys=None, sql_table=None):
		result = "This Dataset had been storaged in DB"
		try:
			if keys is not None and sql_table is not None:
				dataSet.to_sql(table_name, self.engine, if_exists=if_exists, index=False, dtype=dtype)
			if keys is None and sql_table is None:
				self.to_sql_set_primary_key_and_not_null(dataSet, table_name, self.engine,
						if_exists=if_exists, index=False, keys=keys, sql_table=sql_table)
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

	# 儲存爬蟲 log
	# input：log_df 的 type 為 dataFrame
	def save_climate_crawler_log(self, log_df):
		table_name='climate_crawler_log'
		meta = MetaData(self.engine, schema=None)
		sql_table = Table(table_name, meta,
			Column('Station_ID', CHAR(length=6), primary_key=True, nullable=False),
			Column('Station_Area', NCHAR(length=32), nullable=False),
			Column('Reporttime', DateTime, nullable=False),
			Column('Hourly_Start_Period', CHAR(length=10)),
			Column('Hourly_End_Period', CHAR(length=10)),
			Column('Daily_Start_Period', CHAR(length=7)),
			Column('Daily_End_Period', CHAR(length=7)))

		self.to_sql(log_df, table_name, if_exists='replace', keys='Station_ID', sql_table=sql_table)
		print('Save DB: climate crawler log')
		print(log_df)

	def get_last_climate_crawler_log(self):
		select_sql = 'SELECT * FROM climate_crawler_log'
		query_result = self.engine.execute(select_sql).fetchall()
		has_crawler_log = len(query_result) != 0

		if has_crawler_log:
			crawler_log_columns=['Station_ID', 'Station_Area', 'Reporttime', 'Hourly_Start_Period', 'Hourly_End_Period', 'Daily_Start_Period', 'Daily_End_Period']
			crawler_log_df = pd.DataFrame(query_result, columns=crawler_log_columns)\
							   .set_index('Station_ID')\
							   .drop('Reporttime', axis=1)
			print('last climate crawler log:')
			print(crawler_log_df)
			return crawler_log_df
		else:
			return None