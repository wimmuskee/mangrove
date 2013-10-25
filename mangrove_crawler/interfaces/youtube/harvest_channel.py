# -*- coding: utf-8 -*-

import common
from json import dumps
import MySQLdb
from time import sleep, time
from uuid import uuid4
from datetime import datetime
import pytz

class HarvestChannel:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)


	def harvest(self,channel=""):
		c = self.DB.cursor()

		if channel:
			print "Harvesting " + channel
			query = "SELECT youtube_id,updated FROM channels WHERE username = %s"
			c.execute(query, (channel))
			row = c.fetchone()
			if row:
				self.getPage(row[0],row[1])
				self.updateChannelTimestamp(row[0])
			else:
				print "Channel: " + channel + " not found. Add the following:"
				info = common.getChannelInfo(self.config["developer_key"],channel)
				print dumps(info, indent=4)
		else:
			print "Harvesting all channels"
			query = "SELECT youtube_id,updated FROM channels;"
			c.execute(query)
			for row in c.fetchall():
				self.getPage(row[0],row[1],"")
				self.updateChannelTimestamp(row[0])


	def getPage(self,channel_id,fromts,token=""):
		result = common.getChannelPage(self.config["developer_key"],channel_id,fromts,token)
		
		for vid in result["videos"].keys():
			print dumps(result["videos"][vid],4)
			self.storeResult(result["videos"][vid])
		
		if result["meta"]["token"]:
			sleep(5)
			self.getPage(channel_id,fromts,result["meta"]["token"])


	def storeResult(self,video):
		timestamp = int(time())
		c = self.DB.cursor()
		
		""" retrieve by page_id, if exists, update, else insert """
		query = "SELECT * FROM videos WHERE youtube_id = %s"
		c.execute(query, (video["youtube_id"]))
		row = c.fetchone()
		
		if row:
			identifier = row[0]
			query = "UPDATE videos SET title=%s, description=%s, duration=%s, license=%s, thumbnail=%s, embed=%s, publishdate=%s, channel_id=%s WHERE youtube_id = %s"
			c.execute(query, (video["title"],video["description"],video["duration"],video["license"],video["thumbnail"],video["embed"],video["publishdate"],video["channel_id"],video["youtube_id"]))
			c.execute("""UPDATE oairecords SET updated=%s WHERE identifier=%s""", (timestamp,identifier))
		else:
			identifier = uuid4()
			query = "INSERT INTO videos (identifier, youtube_id, title, description, duration, license, thumbnail, embed, publishdate, channel_id) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
			c.execute(query, (identifier,video["youtube_id"],video["title"],video["description"],video["duration"],video["license"],video["thumbnail"],video["embed"],video["publishdate"],video["channel_id"]) )
			c.execute("""INSERT INTO oairecords (identifier,setspec,updated) VALUES ( %s, %s, %s )""", (identifier,"ftc",timestamp))


	def updateChannelTimestamp(self,channel_id):
		d = datetime.utcnow()
		timestamp = d.strftime('%Y-%m-%dT%H:%M:%SZ')
		
		c = self.DB.cursor()
		query = "UPDATE channels SET updated = %s WHERE youtube_id = %s"
		c.execute(query, (timestamp,channel_id))
