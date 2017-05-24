# -*- coding: utf-8 -*-

from storage.mysql import Database
from mangrove_libs.common import getLogger, getRequestsProxy, getHttplib2Proxy
from time import time


class Interface:
	def __init__(self,config):
		self.config = config
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"])
		self.httpProxy=None
		self.logger = getLogger(self.__doc__)
		self.startts = int(time())
		self.DB.setCollectionInfo(config["configuration"])


	def handleRequestsProxy(self):
		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getRequestsProxy(self.config["proxy_host"],self.config["proxy_port"])


	def handleHttplib2Proxy(self):
		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getHttplib2Proxy(self.config["proxy_host"],self.config["proxy_port"])
