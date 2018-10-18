import os

import numpy as np
import pandas as pd

def get_current_time():
	return pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')

def to_csv(dataSet, file_name):
	file_path = 'data/' + file_name
	dataSet.to_csv(file_path, encoding='utf-8', index=False)
	print('==== The ' + file_path, 'is saved ====')

def merge_csv(merge_folder_path, save_file_name):
	save_file_path = 'data/' + save_file_name

	for dirname, dirnames, filenames in os.walk(merge_folder_path):
		if len(filenames) == 0:
			return False

		for idx, filename in enumerate(filenames):
			file = os.path.join(dirname, filename)
			df = pd.read_csv(file)

			if not os.path.isfile(save_file_path):
				df.to_csv(save_file_path, encoding='utf-8', index=False)
			else:
				if idx == 0:
					df.to_csv(save_file_path, encoding='utf-8', index=False)
				else:
					df.to_csv(save_file_path, mode='a', encoding='utf-8', index=False, header=False)

		return True

def merge_climate_data_to_csv():
	merge_daily_climate = merge_csv(merge_folder_path='data/daily_climate', save_file_name='daily_climate_data.csv')
	merge_hourly_climate = merge_csv(merge_folder_path='data/hourly_climate', save_file_name='hourly_climate_data.csv')
	return merge_daily_climate, merge_hourly_climate

def delete_csv(file_name):
	file_path = 'data/' + file_name
	if os.path.exists(file_path):
		os.remove(file_path)
	else:
		print("The {} file does not exist".format(file_path))