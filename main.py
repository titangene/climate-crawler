import logging

from lib.Logging import Logging
from lib.Station_Crawler import Station_Crawler
from lib.Climate_Crawler import Climate_Crawler

def start():
	Logging().setting()
	# 更新目前可用的氣候觀測站
	Station_Crawler().start()
	# 擷取氣候資料，包括 daily 和 hourly 的氣候資料
	Climate_Crawler().start()

def main():
	start()

if __name__ == '__main__':
	main()
