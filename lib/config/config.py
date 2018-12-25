from configparser import ConfigParser, ExtendedInterpolation

class Config:
	def __init__(self):
		self.config = ConfigParser(interpolation=ExtendedInterpolation())
		self.config.read('lib/config/config.ini', encoding='utf-8')

	# 取得 資料庫連接 URL
	def get_db_url(self):
		return self.config['database']['db_url']