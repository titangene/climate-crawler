import requests
from fake_useragent import UserAgent

def set_header_user_agent():
	user_agent = UserAgent()
	return user_agent.random

def get(url):
	user_agent = set_header_user_agent()
	req = requests.get(url, headers={'user-agent': user_agent})
	return req