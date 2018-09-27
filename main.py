import pandas as pd
import numpy as np

from lib.Daily_Climate_Crawler import Daily_Climate_Crawler
from lib.Hourly_Climate_Crawler import Hourly_Climate_Crawler
from lib.db.csv_to_sql import csv_to_mssql

def main():
	daily_crawler = Daily_Climate_Crawler()
	daily_crawler.obtain_daily_data(start_period='2015-1', end_period='2017-12')

	hourly_crawler = Hourly_Climate_Crawler()
	hourly_crawler.obtain_hourly_data(start_period='2015-1-1', end_period='2017-12-31')

	mssql_host_ip = '192.168.191.130:1433'
	DB_name = 'Test_DB'

	to_sql = csv_to_mssql(host_ip=mssql_host_ip, DB_name=DB_name)
	daily_climate_to_sql = to_sql.deal_with_dilay_data(table_name='Dilay_Climate_data', csv_name='daily_climate_data.csv')
	print(daily_climate_to_sql)

	hourly_climate_to_sql = to_sql.deal_with_hourly_data(table_name='Hourly_Climate_data', csv_name='hourly_climate_data.csv')
	print(hourly_climate_to_sql)

if __name__ == '__main__':
	main()
