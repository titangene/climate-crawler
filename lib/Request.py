import time
import logging

import requests
from requests.exceptions import HTTPError
from fake_useragent import UserAgent

from lib.config.config import Config

def set_header_user_agent():
	user_agent = UserAgent()
	return user_agent.random

def get(url):
	is_ok_status = False
	re_request_count = 0
	re_request_max_count = Config().get_re_request_max_count()
	re_request_sleep_time = Config().get_re_request_sleep_time()
	pre_request_sleep_time = Config().get_pre_request_sleep_time()
	while is_ok_status == False:
		try:
			user_agent = set_header_user_agent()
			response = requests.get(url, headers={'user-agent': user_agent})
			time.sleep(pre_request_sleep_time)
			logging.info('response status code ({}) for url: {} | header.user-agent={}'.format(response.status_code, url, user_agent))
			# 若 response 不是 200 就會拋出錯誤
			response.raise_for_status()
			is_ok_status = True
		except HTTPError as e:
			logging.exception(e)
			# 休息幾秒後重新發 request
			time.sleep(re_request_sleep_time)
			re_request_count += 1
			logging.warning('re-request count: {}'.format(re_request_count))

		# 重新發超過 3 次 request 就會報錯並停止執行程式
		if re_request_count >= re_request_max_count:
			raise HTTPError('Non-200 response code ({}) for url: {}'.format(
				response.status_code, url))
	return response