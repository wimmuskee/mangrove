# -*- coding: utf-8 -*-

# http://backstage-docs.openstate.eu/

# for generic docs,
# there are interfaces, the technical stuff, the api's, represented by modules
# a specific interface can be accessed, using a configuration
# within an interface, there can be collections
# collections have records

import common
from mangrove_crawler.common import getRequestsProxy, getLogger
from formatter.nllom import makeLOM, getEmptyLomDict
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
import MySQLdb
import MySQLdb.cursors
from time import sleep, time
from uuid import uuid4
from datetime import datetime


class Harvester:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1,cursorclass=MySQLdb.cursors.DictCursor)
		self.DB.set_character_set('utf8')
		self.httpProxy=None
		self.logger = getLogger('backstage harvester')

		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getRequestsProxy(self.config["proxy_host"],self.config["proxy_port"])


	def harvest(self,collection=""):
		c = self.DB.cursor()
		query = "SELECT * FROM collections WHERE configuration = %s"
		c.execute(query, (self.config["configuration"],))
		row = c.fetchone()
		fromdate = datetime.fromtimestamp(int(row["updated"])).strftime('%Y-%m-%d')
		self.config["collection_id"] = row["id"]
		
		self.getPage(self.config["configuration"],fromdate)
		self.updateCollectionTimestamp(self.config["configuration"])


	def getPage(self,collection,fromdate,token=0):
		result = common.getResultPage(self.httpProxy,collection,fromdate,token)

		for video_id in result["videos"].keys():
			v = result["videos"][video_id]
			r = self.getDefaultRecord()
			# title might also be a combination of program title, title and subtitle, check that later
			r["original_id"] = video_id
			r["title"] = v["item"]["_source"]["title"]
			r["location"] = v["item"]["_source"]["meta"]["original_object_urls"]["html"]
			r["author" ] = v["metadata"]["authors"]
			if "description" in v["item"]["_source"]:
				r["description"] = v["item"]["_source"]["description"]
			if "tags" in v["item"]["_source"]:
				r["keywords"] = v["item"]["_source"]["tags"]
			if "ageGroups" in v["item"]["_source"]:
				r["typicalagerange"] = v["item"]["_source"]["ageGroups"]
			if "tijdsduur" in v["metadata"]:
				r["duration"] = common.getDuration(v["metadata"]["tijdsduur"] )
			
			self.storeResult(r,self.config["configuration"])
		
		if result["meta"]["token"] < result["meta"]["total"]:
			self.logger.debug( "token = " + str(result["meta"]["token"]) + ", total = " + str(result["meta"]["total"]) )
			sleep(5)
			self.getPage(collection,fromdate,result["meta"]["token"])


	def getDefaultRecord(self):
		r = getEmptyLomDict()
		r["publisher"] = self.config["publisher"]
		r["cost"] = "no"
		r["language"] = "nl"
		r["aggregationlevel"] = "2"
		r["metalanguage"] = "nl"
		r["learningresourcetype"] = "informatiebron"
		r["intendedenduserrole"] = "learner"
		return r


	def getOaidcRecord(self,record):
		r = getEmptyOaidcDict()
		r["title"] = record["title"]
		r["description"] = record["description"]
		r["subject"] = record["keywords"]
		r["publisher"] = record["publisher"]
		r["identifier"] = record["location"]
		r["language"] = record["language"]
		return r


	def storeResult(self,record,setspec):
		lom = makeLOM(record)
		oaidc = makeOAIDC(self.getOaidcRecord(record))
		timestamp = int(time())
		c = self.DB.cursor()
		
		""" retrieve by page_id, if exists, update, else insert """
		query = "SELECT updated FROM oairecords WHERE original_id = \"" + record["original_id"] + "\""
		c.execute(query)
		row = c.fetchone()
		
		if row:
			query = "UPDATE oairecords SET updated=%s, lom=%s, oaidc=%s WHERE original_id=%s"
			c.execute(query,(timestamp,lom,oaidc,record["original_id"]))
		else:
			identifier = uuid4()
			query = "INSERT INTO oairecords (identifier,original_id,collection_id,setspec,updated,lom,oaidc) VALUES ( %s, %s, %s, %s, %s, %s, %s )"
			c.execute(query, (identifier,record["original_id"],self.config["collection_id"],setspec,timestamp,lom,oaidc))

		self.DB.commit()


	def updateCollectionTimestamp(self,configuration,):
		timestamp = int(time())
		c = self.DB.cursor()
		query = "UPDATE collections SET updated = %s WHERE configuration = %s"
		c.execute(query, (timestamp,configuration))
		self.DB.commit()
