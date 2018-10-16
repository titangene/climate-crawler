from sqlalchemy import Table, Column, DateTime, CHAR, NCHAR
from sqlalchemy.schema import MetaData
import pandas as pd
import numpy as np

class Climate_Crawler_Log:
	def __init__(self, to_mssql):
		self.to_mssql = to_mssql
		self.sql_engine = self.to_mssql.engine
		self.table_name = 'climate_crawler_log'
		self.log_columns = ['Station_ID', 'Station_Area', 'Reporttime', 'Hourly_Start_Period', 'Hourly_End_Period', 'Daily_Start_Period', 'Daily_End_Period']

		self.sql_table = self.set_sql_table()

	def set_sql_table(self):
		meta = MetaData(self.sql_engine, schema=None)
		sql_table = Table(self.table_name, meta,
				Column('Station_ID', CHAR(length=6), primary_key=True, nullable=False),
				Column('Station_Area', NCHAR(length=32), nullable=False),
				Column('Reporttime', DateTime, nullable=False),
				Column('Hourly_Start_Period', CHAR(length=10)),
				Column('Hourly_End_Period', CHAR(length=10)),
				Column('Daily_Start_Period', CHAR(length=7)),
				Column('Daily_End_Period', CHAR(length=7)))
		return sql_table

	# 儲存爬蟲 log
	# input：log_df 的 type 為 dataFrame
	def save_climate_crawler_log(self, log_df):
		self.to_mssql.to_sql(log_df, self.table_name, if_exists='replace', keys='Station_ID', sql_table=self.sql_table)
		print('Save DB: climate crawler log')
		print(log_df)

	def get_climate_crawler_log(self):
		select_sql = 'SELECT * FROM {}'.format(self.table_name)
		query_result = self.sql_engine.execute(select_sql).fetchall()
		has_crawler_log = len(query_result) != 0

		if has_crawler_log:
			crawler_log_df = pd.DataFrame(query_result, columns=self.log_columns)\
									.set_index('Station_ID')\
									.drop('Reporttime', axis=1)
			print('last climate crawler log:')
			print(crawler_log_df)
			return crawler_log_df
		else:
			return None