import pandas as pd
import numpy as np

from lib.Daily_Climate_Crawler import Daily_Climate_Crawler
from lib.Hourly_Climate_Crawler import Hourly_Climate_Crawler
from lib.db.csv_to_sql import csv_to_mssql

def main():
	daily_crawler = Daily_Climate_Crawler()
	daily_crawler.all_station_id = daily_crawler.all_station_id[414:418]
	daily_crawler.obtain_daily_data(start_period='2017-12', end_period='2017-12')
	# daily_crawler.obtain_daily_data(start_period='2015-1', end_period='2017-12')

	# 某觀測站 在 某月內 每天 的氣候資料
	# daily_climate_url = daily_crawler.climate_station.get_daily_full_url('2017-12', 'C0Z230')
	# daily_crawler_df = daily_crawler.catach_climate_data(daily_climate_url)
	# daily_crawler_df.to_csv('data/daily_climate.csv', index=False, encoding='utf8')
	# print(daily_crawler_df)

	hourly_crawler = Hourly_Climate_Crawler()
	hourly_crawler.all_station_id = hourly_crawler.all_station_id[420:424]
	hourly_crawler.obtain_hourly_data(start_period='2017-12-30', end_period='2017-12-31')
	# hourly_crawler.obtain_hourly_data(start_period='2015-1-1', end_period='2017-12-31')

	mssql_host_ip = '192.168.191.130:1433'
	DB_name = 'Test_DB'
	to_sql = csv_to_mssql(host_ip=mssql_host_ip, DB_name=DB_name)
	clear_old_data(to_sql)
	to_sql.deal_with_daily_and_hourly_data()

def clear_old_data(to_sql):
	try:
		connection = to_sql.engine.connect()
		connection.execute('DROP TABLE Daily_Climate_data')
		connection.execute('CREATE TABLE [dbo].[Daily_Climate_data]([UUID] [nchar](80) NOT NULL,[Area] [nchar](32) NULL,[Temperature] [float] NULL,[Max_T] [float] NULL,[Min_T] [float] NULL,[Humidity] [float] NULL,[Reporttime] [datetime] NULL,PRIMARY KEY ([UUID]))')

		connection.execute('DROP TABLE Hourly_Climate_data')
		connection.execute('CREATE TABLE [dbo].[Hourly_Climate_data]([UUID] [nchar](80) NOT NULL,[Area] [nchar](32) NULL,[Temperature] [float] NULL,[Humidity] [float] NULL,[Reporttime] [datetime] NULL,PRIMARY KEY ([UUID]))')
		connection.close()
	except Exception as e:
		result = e
	else:
		result = 'drop table and create table: OKAY'
	print(result)

if __name__ == '__main__':
	main()
