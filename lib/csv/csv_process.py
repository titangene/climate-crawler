import numpy as np
import pandas as pd

def get_current_time():
	return pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')

def to_csv(dataSet, file_name):
	file_path = 'data/' + file_name
	dataSet.to_csv(file_path, encoding='utf-8', index=False)
	print('==== The ' + file_path, 'is saved ====')