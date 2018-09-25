from lib.Station_Crawler import Station_Crawler

def main():
	station_crawler = Station_Crawler()
	station_crawler.start()
	print(station_crawler.climate_station_df.head())

if __name__ == '__main__':
	main()