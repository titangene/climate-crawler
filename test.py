from sqlalchemy.orm import sessionmaker
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

def clear_old_data(engine):
	Session = sessionmaker(bind=engine)
	session = Session()
	session.execute('TRUNCATE TABLE Daily_Climate_data')
	session.execute('TRUNCATE TABLE Hourly_Climate_data')
	session.execute('TRUNCATE TABLE climate_crawler_log')
	session.commit()
	session.close()
	print('TRUNCATE TABLE: OKAY')

if __name__ == '__main__':
	main()
