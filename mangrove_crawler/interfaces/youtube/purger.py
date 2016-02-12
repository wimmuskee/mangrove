# -*- coding: utf-8 -*-
from common import getVideoAvailableStatus
from mangrove_crawler.common import getHttplib2Proxy, getLogger
from storage.mysql import Database
from time import sleep


class Purger:
	def __init__(self,config):
		self.config = config
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"],config["configuration"])
		self.httpProxy=None
		self.logger = getLogger('youtube purger')

		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getHttplib2Proxy(self.config["proxy_host"],self.config["proxy_port"])


	def purge(self,method="",part=None):
		self.logger.info("Purging all youtube video's that are not available.")
		
		for row in self.DB.getUndeleted():
			if not getVideoAvailableStatus(self.httpProxy,self.config["developer_key"],row["original_id"]):
				self.logger.info("purging: " + row["identifier"])
				self.DB.deleteRecord(row["identifier"])
			
			sleep(1)
