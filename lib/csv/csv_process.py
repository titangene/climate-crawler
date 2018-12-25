import os
import re

import numpy as np
import pandas as pd

from lib.Climate_Common import get_current_time

def createFolder(directory):
	try:
		if not os.path.exists(directory):
			os.makedirs(directory)
			print('create directory:', directory)
	except OSError:
		print('Error: Creating directory:', directory)

def get_file_folder_path(file_path):
	regex_match = re.search(r'data\/?.*\/', file_path)
	return regex_match.group()

def set_data_path(csv_name):
	file_path = 'data/'+ csv_name
	return file_path

def load_csv(csv_name):
	file_path = set_data_path(csv_name)
	dataSet = pd.read_csv(file_path)
	return dataSet

def to_csv(dataSet, csv_name, mode='w', header=True, backup=False):
	file_path = set_data_path(csv_name)
	file_folder_path = get_file_folder_path(file_path)
	createFolder(file_folder_path)
	dataSet.to_csv(file_path, encoding='utf-8', index=False, mode=mode, header=header)
	if mode != 'a':
		print('==== The ' + file_path, 'is saved ====')
	if backup:
		to_csv_backup(dataSet, csv_name)

def to_csv_backup(dataSet, csv_name):
	current_time = get_current_time()
	file_path = 'data/backup/{}_{}'.format(current_time, csv_name)
	createFolder('data/backup')
	dataSet.to_csv(file_path, encoding='utf-8', index=False)

def merge_csv(merge_folder_path, save_file_name):
	for dirname, dirnames, filenames in os.walk(merge_folder_path):
		if len(filenames) == 0:
			return False

		for idx, filename in enumerate(filenames):
			file = os.path.join(dirname, filename)
			df = pd.read_csv(file)

			# 用第一個 csv 的某觀測站資料 將 氣候資料 複寫，第一筆之後用 append
			if idx == 0:
				to_csv(df, save_file_name)
			else:
				to_csv(df, save_file_name, mode='a', header=False)

		save_file_full_path = set_data_path(save_file_name)
		merge_df = pd.read_csv(save_file_full_path)
		to_csv_backup(merge_df, save_file_name)
		return True

def merge_climate_data_to_csv():
	is_merge_daily_climate = merge_csv(merge_folder_path='data/daily_climate', save_file_name='daily_climate_data.csv')
	is_merge_hourly_climate = merge_csv(merge_folder_path='data/hourly_climate', save_file_name='hourly_climate_data.csv')
	return is_merge_daily_climate, is_merge_hourly_climate

def delete_csv(csv_name):
	file_path = set_data_path(csv_name)
	if os.path.exists(file_path):
		os.remove(file_path)
	else:
		print("The {} file does not exist".format(file_path))