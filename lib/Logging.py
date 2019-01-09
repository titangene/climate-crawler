import logging

from lib.Climate_Common import get_current_time
from lib.csv import csv_process

class Logging:
	def setting(self):
		FORMAT = '%(asctime)s %(levelname)s: %(message)s'

		current_time = get_current_time()

		csv_process.createFolder('data/backup_crawler_log')

		file_path = 'data/crawler.log'
		backup_file_path = 'data/backup_crawler_log/{}_crawler.log'.format(current_time)

		file_handler = [
			logging.FileHandler(file_path, 'w', 'utf-8'),
			logging.FileHandler(backup_file_path, 'w', 'utf-8')
		]
		logging.basicConfig(handlers=file_handler, format=FORMAT, level=logging.INFO)