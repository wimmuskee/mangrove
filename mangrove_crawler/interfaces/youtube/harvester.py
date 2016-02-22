# -*- coding: utf-8 -*-
import common
from mangrove_crawler.common import getHttplib2Proxy, getLogger
import json
from storage.mysql import Database
from time import sleep, time
from formatter.nllom import makeLOM, getEmptyLomDict, formatDurationFromSeconds
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict


class Harvester:
	def __init__(self,config):
		self.config = config
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"],config["configuration"])
		self.httpProxy=None
		self.logger = getLogger('youtube harvester')

		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getHttplib2Proxy(self.config["proxy_host"],self.config["proxy_port"])


	def harvest(self,part=None):
		self.logger.info("Harvesting all channels")
		startts = int(time())
		f = open("youtubechannels.json", 'r')
		channels = json.loads(f.read() )
		f.close()
		
		for channel in channels:
			self.getPage(channel,"")
		
		self.DB.touchCollection(startts)


	def getPage(self,channel,token=""):
		result = common.getChannelPage(self.httpProxy,self.config["developer_key"],channel["youtube_id"],self.DB.collection_updated,token)
		
		for vid in result["videos"].keys():
			video = result["videos"][vid]
			self.logger.debug(video["youtube_id"] + " - " + video["title"])
			r = self.getDefaultLomRecord()
			r["original_id"] = video["youtube_id"]
			r["identifier"].append( { "catalog": "YouTube", "value": video["youtube_id"] } )
			r["identifier"].append( { "catalog": "URI", "value": "http://youtu.be/" + video["youtube_id"] } )
			r["title"] = video["title"]
			r["description"] = video["description"]
			r["keywords"] = channel["keyword"]
			r["language"] = channel["language"]
			r["metalanguage"] = channel["language"]
			r["location"] = "http://youtu.be/" + video["youtube_id"]
			r["publishdate"] = video["publishdate"]
			r["author"].append( { "fn": channel["title"], "url": "http://www.youtube.com/user/" + channel["username"] } )
			r["duration"] = formatDurationFromSeconds(video["duration"])
			r["context"] = channel["context"]
			r["learningresourcetype"] = channel["learningresourcetype"]
			r["intendedenduserrole"] = channel["intendedenduserrole"]
			if video["license"] == "creativeCommon":
				r["copyright"] = "cc-by-30"
			elif video["license"] == "youtube":
				r["copyright"] = "Standaard YouTube licentie: http://www.youtube.com/t/terms"
			if video["embed"]:
				r["embed"] = "http://www.youtube.com/embed/" + video["youtube_id"]
			r["thumbnail"] = video["thumbnail"]
			if channel["discipline"]:
				r["discipline"].append([channel["discipline"]])
			
			self.storeResult(r,channel["setspec"])
		
		if result["meta"]["token"]:
			sleep(5)
			self.getPage(channel,result["meta"]["token"])


	def getDefaultLomRecord(self):
		r = getEmptyLomDict()
		r["publisher"] = "YouTube"
		r["cost"] = "no"
		r["format"] = "video/x-flv"
		r["aggregationlevel"] = "2"
		r["publishdate"] = "1970-01-01T00:00:00Z"
		return r


	def getOaidcRecord(self,record):
		r = getEmptyOaidcDict()
		r["title"] = record["title"]
		r["description"] = record["description"]
		r["subject"] = record["keywords"]
		r["publisher"] = record["publisher"]
		r["format"] = record["format"]
		r["identifier"] = record["location"]
		r["language"] = record["language"]
		r["rights"] = record["copyright"]
		return r


	def storeResult(self,record,setspec):
		lom = makeLOM(record)
		oaidc = makeOAIDC(self.getOaidcRecord(record))

		""" retrieve by page_id, if exists, update, else insert """
		row = self.DB.getUpdatedByOriginalId(record["original_id"])
		
		if row:
			self.DB.updateRecord(lom,oaidc,record["original_id"])
		else:
			self.DB.insertRecord(lom,oaidc,setspec,record["original_id"])
