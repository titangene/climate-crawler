#coding=utf-8

import pandas as pd
import numpy as np

from lib.Daily_Climate_Crawler import Daily_Climate_Crawler
from lib.Hourly_Climate_Crawler import Hourly_Climate_Crawler

def main():
	daily_crawler = Daily_Climate_Crawler()
	daily_crawler.obtain_daily_data(start_period='2016-12', end_period='2017-1')

	hourly_crawler = Hourly_Climate_Crawler()
	hourly_crawler.obtain_hourly_data(start_period='2017-12-31', end_period='2018-01-01')

if __name__ == '__main__':
	main()
