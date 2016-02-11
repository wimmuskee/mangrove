# -*- coding: utf-8 -*-

from mangrove_crawler.interfaces.youtube.common import getVideoAvailableStatus
from mangrove_crawler.common import getHttplib2Proxy, getLogger
import MySQLdb
import MySQLdb.cursors
from time import sleep, time


class Purger:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1,cursorclass=MySQLdb.cursors.DictCursor)
		self.DB.set_character_set('utf8')
		self.httpProxy=None
		self.logger = getLogger('khanacademy purger')

		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getHttplib2Proxy(self.config["proxy_host"],self.config["proxy_port"])


	# keeping method and part for now, for backwards compatibility with youtube
	def purge(self,method="",part=None):
		c = self.DB.cursor()
		
		self.logger.info("Purging all khanacademy video's that are not available.")
		query = "SELECT identifier,original_id FROM oairecords WHERE deleted = 0"
		c.execute(query)

		for row in c.fetchall():
			timestamp = int(time())
			if not getVideoAvailableStatus(self.httpProxy,self.config["developer_key"],row["original_id"]):
				self.logger.info("purging: " + row["identifier"])
				c.execute("""UPDATE oairecords SET updated=%s, deleted=1 WHERE identifier=%s""", (timestamp,row["identifier"]))
			
			sleep(1)
