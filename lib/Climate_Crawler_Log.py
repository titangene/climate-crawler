import pandas as pd
import numpy as np

from lib.config.config import Config
import lib.Climate_Common as Climate_Common

class Climate_Crawler_Log:
	def __init__(self, to_mssql):
		self.to_mssql = to_mssql
		self.sql_engine = self.to_mssql.sql_engine
		self.table_name = Config().get_database_table_name_for_crawler_log()
		self.period_columns = ['Daily_Start_Period', 'Daily_End_Period', 'Hourly_Start_Period', 'Hourly_End_Period']
		self.new_period_columns = self.set_new_period_columns()
		self.sql_columns = ['Station_ID', 'Station_Area', 'Reporttime'] + self.period_columns

		self.sql_table = self.set_sql_table()
		# 取得爬蟲 log dataFrame
		self.log_df = self.get_log()
		# 設定新的 start 和 end period
		self.log_df = self.set_new_period_columns_in_dataFrame(self.log_df)

	# 新增 新的 period 欄位 (在前面加上 'New')
	# e.g. 'Daily_Start_Period' --> 'New_Daily_Start_Period'
	def set_new_period_columns(self):
		return list(map(lambda col: 'New_' + col, self.period_columns))

	def set_sql_table(self):
		from sqlalchemy.schema import MetaData
		from sqlalchemy import Table, Column, DateTime, CHAR, NCHAR

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

	def get_log(self):
		select_sql = 'SELECT * FROM {}'.format(self.table_name)
		query_result = self.sql_engine.execute(select_sql).fetchall()
		has_crawler_log = len(query_result) != 0

		if has_crawler_log:
			crawler_log_df = pd.DataFrame(query_result, columns=self.sql_columns)
			crawler_log_df['Station_Area'] = crawler_log_df['Station_Area'].apply(lambda x: x.strip())
		else:
			crawler_log_df = pd.DataFrame(columns=self.sql_columns)
		print('\n# DB: \nlast climate crawler log:')
		print(crawler_log_df)
		return crawler_log_df

	# 設定新的 start 和 end period
	def set_new_period_columns_in_dataFrame(self, log_df):
		if self.log_df.empty:
			log_df = self.create_empty_log_dataFrame()
			return log_df
		else:
			log_df = self.create_new_period_column_in_dataFrame(log_df)
			return log_df

	def create_new_period_column_in_dataFrame(self, log_df):
		create_new_period_column_in_dataFrame = Climate_Common.get_yesterday_date_str()
		log_df['New_Daily_Start_Period'] = log_df['Daily_End_Period'].apply(self.set_start_period)
		log_df['New_Daily_End_Period'] = create_new_period_column_in_dataFrame
		log_df['New_Hourly_Start_Period'] = log_df['Hourly_End_Period'].apply(self.set_start_period)
		log_df['New_Hourly_End_Period'] = create_new_period_column_in_dataFrame
		return log_df.set_index('Station_ID')

	def create_empty_log_dataFrame(self):
			columns = self.sql_columns + self.new_period_columns
			log_df = pd.DataFrame(columns=columns).set_index('Station_ID')
			return log_df

	def set_start_period(self, period):
		if period:
			return Climate_Common.add_one_day_str(period)
		else:
			return Climate_Common.get_recent_climate_data_start_period()

	def save_log(self, log_df):
		self.to_mssql.to_sql(log_df, self.table_name, if_exists='replace', keys='Station_ID', sql_table=self.sql_table)

	def update_log_dataFrame(self, log_df):
		rename_columns = dict(zip(self.new_period_columns, self.period_columns))
		log_df = log_df.drop(self.period_columns, axis=1)\
				.rename(columns=rename_columns)\
				.reset_index()
		return log_df