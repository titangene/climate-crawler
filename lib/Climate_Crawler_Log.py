from sqlalchemy import Table, Column, DateTime, CHAR, NCHAR
from sqlalchemy.schema import MetaData
import pandas as pd
import numpy as np

import lib.Climate_Common as Climate_Common

class Climate_Crawler_Log:
	def __init__(self, to_mssql):
		self.to_mssql = to_mssql
		self.sql_engine = self.to_mssql.sql_engine
		self.table_name = 'climate_crawler_log'
		self.log_columns_period = ['Daily_Start_Period', 'Daily_End_Period', 'Hourly_Start_Period', 'Hourly_End_Period']
		self.log_columns = ['Station_ID', 'Station_Area', 'Reporttime'] + self.log_columns_period

		self.sql_table = self.set_sql_table()

		# 爬蟲 log dataFrame
		self.log_df = self.get_climate_crawler_log()
		# 設定新的 start 和 end period
		self.log_df = self.set_new_period(self.log_df)

	def set_sql_table(self):
		meta = MetaData(self.sql_engine, schema=None)
		sql_table = Table(self.table_name, meta,
				Column('Station_ID', CHAR(length=6), primary_key=True, nullable=False),
				Column('Station_Area', NCHAR(length=32), nullable=False),
				Column('Reporttime', DateTime, nullable=False),
				Column('Daily_Start_Period', CHAR(length=10)),
				Column('Daily_End_Period', CHAR(length=10)),
				Column('Hourly_Start_Period', CHAR(length=10)),
				Column('Hourly_End_Period', CHAR(length=10)))
		return sql_table

	# 儲存爬蟲 log
	# input：log_df 的 type 為 dataFrame
	def save_climate_crawler_log(self, log_df):
		self.to_mssql.to_sql(log_df, self.table_name, if_exists='replace', keys='Station_ID', sql_table=self.sql_table)
		print(log_df)

	def get_climate_crawler_log(self):
		select_sql = 'SELECT * FROM {}'.format(self.table_name)
		query_result = self.sql_engine.execute(select_sql).fetchall()
		has_crawler_log = len(query_result) != 0

		if has_crawler_log:
			crawler_log_df = pd.DataFrame(query_result, columns=self.log_columns).set_index('Station_ID')
			print('last climate crawler log:')
			print(crawler_log_df)
			return crawler_log_df
		else:
			return None

	# 建立空的 爬蟲 log dataFrame
	def create_empty_dataFrame(self):
		log_df = pd.DataFrame(columns=self.log_columns)\
				   .set_index('Station_ID')
		return log_df

	# 設定新的 start 和 end period
	def set_new_period(self, log_df):
		if self.log_df is None:
			log_df = self.create_empty_dataFrame()
		else:
			log_df['New_Daily_Start_Period'] = log_df['Daily_End_Period'].apply(lambda period: Climate_Common.add_one_day_str(period))
			log_df['New_Daily_End_Period'] = Climate_Common.get_yesterday_date_str()
			log_df['New_Hourly_Start_Period'] = log_df['Hourly_End_Period'].apply(lambda period: Climate_Common.add_one_day_str(period))
			log_df['New_Hourly_End_Period'] = Climate_Common.get_yesterday_date_str()

		return log_df

	def update_dataFrame(self, log_df):
		new_period_columns = ['New_Daily_Start_Period', 'New_Daily_End_Period', 'New_Hourly_Start_Period', 'New_Hourly_End_Period']
		rename_columns = dict(zip(new_period_columns, self.log_columns_period))
		log_df = log_df.drop(self.log_columns_period, axis=1)\
				.rename(columns=rename_columns)\
				.reset_index()
		return log_df