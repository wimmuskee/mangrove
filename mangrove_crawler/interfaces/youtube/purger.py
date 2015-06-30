# -*- coding: utf-8 -*-

import common
from mangrove_crawler.common import getHttplib2Proxy
import MySQLdb
from time import sleep, time


class Purger:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
		self.DB.set_character_set('utf8')
		self.httpProxy=None

		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getHttplib2Proxy(self.config["proxy_host"],self.config["proxy_port"])


	def purge(self,method="",part=None):
		if method == "sync":
			self.sync()
		if method == "channel":
			self.channel(part)
		if method == "file":
			print("not working atm")
		if method == "record_id":
			print("not working atm")


	def sync(self):
		timestamp = int(time())
		c = self.DB.cursor()
		
		print("Purging all youtube video's that are not available.")
		query = "SELECT identifier,youtube_id FROM videos WHERE available = 1"
		c.execute(query)

		for row in c.fetchall():
			identifier = row[0]
			if not common.getVideoAvailableStatus(self.httpProxy,self.config["developer_key"],row[1]):
				print(identifier)
				c.execute("""UPDATE videos SET available=0 WHERE identifier=%s""", (identifier,))
				c.execute("""UPDATE oairecords SET updated=%s, deleted=1 WHERE identifier=%s""", (timestamp,identifier))

			sleep(5)


	def channel(self,part):
		if not part:
			print("part is required for channel purge")
			quit()
		
		timestamp = int(time())
		c = self.DB.cursor()
		
		print("Purging all " + part + " youtube video's.")
		
		# getting channel_id
		c.execute("""SELECT youtube_id FROM channels WHERE username = %s""", (part,))
		row = c.fetchone()
		if row:
			channel_id = row[0]
		else:
			print("Channel not found, exiting")
			quit()
		
		# get channel records
		c.execute("""SELECT identifier,youtube_id FROM videos WHERE available = 1 and channel_id = %s""", (channel_id,))
		
		for row in c.fetchall():
			identifier = row[0]
			print(identifier)
			c.execute("""UPDATE videos SET available=0 WHERE identifier=%s""", (identifier,))
			c.execute("""UPDATE oairecords SET updated=%s, deleted=1 WHERE identifier=%s""", (timestamp,identifier))
			sleep(1)
