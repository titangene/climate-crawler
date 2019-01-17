from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
from pandas.tseries.offsets import MonthEnd

from lib.Station_Crawler import Station_Crawler
from lib.Climate_Crawler import Climate_Crawler

from lib.Climate_Crawler_Log import Climate_Crawler_Log
from lib.db.csv_to_sql import csv_to_mssql

def start():
	# 更新目前可用的氣候觀測站
	Station_Crawler().start()

	to_mssql = csv_to_mssql()
	# 刪除 DB 內所有氣候資料，包括 daily 和 hourly 的氣候資料
	clear_db_all_climate_data(to_mssql.sql_engine)
	# 模擬 無 爬蟲 log
	simulation_without_crawler_log(to_mssql.sql_engine)
	# 模擬 有 爬蟲 log
	# simulation_with_crawler_log(to_mssql)

	climate_crawler = Climate_Crawler()
	# 便於測試用，減少抓氣候資輛的數量
	reduce_number_catch_climate_data(climate_crawler)
	# 抓氣候資料，包括 daily 和 hourly 的氣候資料
	climate_crawler.start()

	db_hourly_data_length, db_daily_data_length = get_db_climate_data_length(to_mssql.sql_engine)
	print('sql - hourly_data_length: ', db_hourly_data_length, '| daily_data_length:', db_daily_data_length)

	csv_hourly_data_length, csv_daily_data_length = get_csv_climate_data_length()
	print('csv - hourly_data_length: ', csv_hourly_data_length, '| daily_data_length:', csv_daily_data_length)

def main():
	start()

# 模擬 無 爬蟲 log
def simulation_without_crawler_log(sql_engine):
	clear_db_climate_log(sql_engine)
	print('\n# simulation: \nlast climate crawler log:', None)

# 模擬 有 爬蟲 log
def simulation_with_crawler_log(to_mssql):
	today_time = pd.Timestamp.now()
	log_df = pd.DataFrame({
		'Station_ID': ['466880', '466900'],
		'Station_Area': ['新北市-板橋', '新北市-淡水'],
		'Reporttime':  [today_time, today_time],
		'Daily_Start_Period': ['2018-10-22', '2018-10-22'],
		'Daily_End_Period': [late_last_month(), a_few_days_ago(day=3)],
		'Hourly_Start_Period': ['2018-10-22', '2018-10-22'],
		'Hourly_End_Period': [a_few_days_ago(day=3), a_few_days_ago(day=3)]
	})
	climate_crawler_Log = Climate_Crawler_Log(to_mssql)
	climate_crawler_Log.save_log(log_df)
	print('\n# simulation: \nlast climate crawler log:')
	print(log_df)

# 便於測試用，減少抓氣候資輛的數量
def reduce_number_catch_climate_data(climate_crawler):
	# 便於測試用，可指定抓特定觀測站的資料
	climate_crawler.station_df = climate_crawler.station_df.head(3)
	# 便於測試用，可指定當某觀測站沒有紀錄爬蟲 log 時，預抓的資料時間範圍
	climate_crawler.recent_climate_data_daily_start_period = a_few_days_ago(month=1, day=3)[:-3]
	climate_crawler.recent_climate_data_hourly_start_period = a_few_days_ago(day=3)

def get_db_climate_data_length(sql_engine):
	select_sql_daily = 'SELECT * FROM Daily_Climate_data'
	select_sql_hourly = 'SELECT * FROM Hourly_Climate_data'
	hourly_query_result = sql_engine.execute(select_sql_daily).fetchall()
	daily_query_result = sql_engine.execute(select_sql_hourly).fetchall()
	hourly_data_length = len(hourly_query_result)
	daily_data_length = len(daily_query_result)
	return hourly_data_length, daily_data_length

def get_csv_climate_data_length():
	daily_climate_df = pd.read_csv('data/daily_climate_data.csv')
	hourly_climate_df = pd.read_csv('data/hourly_climate_data.csv')
	hourly_data_length = len(daily_climate_df)
	daily_data_length = len(hourly_climate_df)
	return hourly_data_length, daily_data_length

# 刪除 DB 內所有氣候資料，包括 daily 和 hourly 的氣候資料
def clear_db_all_climate_data(sql_engine):
	Session = sessionmaker(bind=sql_engine)
	session = Session()
	session.execute('TRUNCATE TABLE Daily_Climate_data')
	session.execute('TRUNCATE TABLE Hourly_Climate_data')
	session.commit()
	session.close()
	print('TRUNCATE TABLE: Daily_Climate_data --> OKAY')
	print('TRUNCATE TABLE: Hourly_Climate_data --> OKAY')

def clear_db_climate_log(sql_engine):
	Session = sessionmaker(bind=sql_engine)
	session = Session()
	session.execute('TRUNCATE TABLE climate_crawler_log')
	session.commit()
	session.close()
	print('TRUNCATE TABLE: climate_crawler_log --> OKAY')

def late_last_month(day=2, month=0):
	today_time = pd.Timestamp.now()
	return (today_time - MonthEnd(1) - pd.DateOffset(day)).strftime('%Y-%m-%d')

def a_few_days_ago(day, month=0):
	today_time = pd.Timestamp.now()
	return (today_time - pd.DateOffset(months=month, days=day)).strftime('%Y-%m-%d')

if __name__ == '__main__':
	main()
