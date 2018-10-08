import pandas as pd
import numpy as np

from lib.Station_Crawler import Station_Crawler
from lib.Climate_Crawler import Climate_Crawler

def start():
	# 更新目前可用的氣候觀測站
	Station_Crawler().start()

	climate_crawler = Climate_Crawler()
	# 便於測試用，可讓爬蟲指定抓特定觀測站的資料，指定觀測站一定要是 list
	climate_crawler.daily_crawler.all_station_id = climate_crawler.daily_crawler.all_station_id[0:1]
	climate_crawler.hourly_crawler.all_station_id = climate_crawler.hourly_crawler.all_station_id[0:1]
	# 抓氣候資料，包括 daily 和 hourly
	climate_crawler.start()

def main():
	start()

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
