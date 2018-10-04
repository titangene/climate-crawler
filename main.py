import pandas as pd
import numpy as np

from lib.Station_Crawler import Station_Crawler
from lib.Daily_Climate_Crawler import Daily_Climate_Crawler
from lib.Hourly_Climate_Crawler import Hourly_Climate_Crawler
from lib.db.csv_to_sql import csv_to_mssql

def main():
	daily_crawler = Daily_Climate_Crawler()
	daily_crawler.obtain_daily_data(start_period='2015-1', end_period='2017-12')

	hourly_crawler = Hourly_Climate_Crawler()
	hourly_crawler.obtain_hourly_data(start_period='2015-1-1', end_period='2017-12-31')

	to_sql = csv_to_mssql()
	to_sql.deal_with_daily_and_hourly_data()

if __name__ == '__main__':
	main()
