# -*- coding: utf-8 -*-

# http://backstage-docs.openstate.eu/

# for generic docs,
# there are interfaces, the technical stuff, the api's, represented by modules
# a specific interface can be accessed, using a configuration
# within an interface, there can be collections
# collections have records

import common
from mangrove_crawler.common import getRequestsProxy, getLogger
import MySQLdb
from time import sleep, time
from uuid import uuid4
from datetime import datetime


class Harvester:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
		self.DB.set_character_set('utf8')
		self.httpProxy=None
		self.logger = getLogger('backstage harvester')

		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getRequestsProxy(self.config["proxy_host"],self.config["proxy_port"])


	def harvest(self,collection=""):
		c = self.DB.cursor()

		if collection:
			self.logger.info("harvesting " + collection)
			query = "SELECT SUBSTRING(updated, 1, 10) AS fromdate, setspec FROM collections WHERE configuration = %s AND collection = %s"
			c.execute(query, (self.config["configuration"],collection))
			row = c.fetchone()
			
			if row:
				self.getPage(collection,row[0],row[1])
				self.updateCollectionTimestamp(self.config["configuration"],collection)
			else:
				self.logger.warn("collection not found: " + collection)
		else:
			self.logger.warn("only collection-based harvesting is implemented")


	def getPage(self,collection,fromdate,setspec,token=0):
		result = common.getResultPage(self.httpProxy,collection,fromdate,token)

		for vid in result["videos"].keys():
			self.storeResult(result["videos"][vid],collection,setspec)
		
		if result["meta"]["token"] < result["meta"]["total"]:
			self.logger.debug( "token = " + str(result["meta"]["token"]) + ", total = " + str(result["meta"]["total"]) )
			sleep(5)
			self.getPage(collection,fromdate,setspec,result["meta"]["token"])
			


	def storeResult(self,video,collection,setspec):
		timestamp = int(time())
		c = self.DB.cursor()
		
		""" retrieve by page_id, if exists, update, else insert """
		query = "SELECT * FROM records WHERE catalogentry = %s AND catalog = %s"
		c.execute(query, (video["id"],self.config["configuration"]))
		row = c.fetchone()
		
		if row:
			identifier = row[0]
			self.logger.debug("updating video: " + str(video["id"]))
			query = "UPDATE records SET title=%s, description=%s, location=%s, duration=%s WHERE identifier = %s"
			c.execute(query, (video["title"],video["description"],video["location"],video["duration"],identifier))
			c.execute("""UPDATE oairecords SET updated=%s WHERE identifier=%s""", (timestamp,identifier))
		else:
			identifier = uuid4()
			self.logger.debug("saving video: " + str(video["id"]))
			query = "INSERT INTO records (identifier, catalog, catalogentry, collection, title, description, location, duration) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )"
			c.execute(query, (identifier,self.config["configuration"],video["id"],collection,video["title"],video["description"],video["location"],video["duration"]))
			c.execute("""INSERT INTO oairecords (identifier,setspec,updated) VALUES ( %s, %s, %s )""", (identifier,setspec,timestamp))
		
		self.DB.commit()


	def updateCollectionTimestamp(self,configuration,collection):
		d = datetime.utcnow()
		timestamp = d.strftime('%Y-%m-%dT%H:%M:%SZ')
		
		c = self.DB.cursor()
		query = "UPDATE collections SET updated = %s WHERE configuration = %s AND collection = %s"
		c.execute(query, (timestamp,configuration,collection))
