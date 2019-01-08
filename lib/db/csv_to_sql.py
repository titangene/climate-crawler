import logging

from sqlalchemy import create_engine
from pandas.io.sql import SQLDatabase, SQLTable
import pandas as pd
import numpy as np

from lib.config.config import Config
from lib.csv.csv_process import load_csv

class csv_to_mssql:
	def __init__(self):
		self.sql_engine = self.create_engine()

	def create_engine(self):
		db_url = Config().get_db_url()
		return create_engine(db_url)

	def disconnect(self):
		self.sql_engine.dispose()

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
		logging.info('to_sql: {}'.format(result))

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