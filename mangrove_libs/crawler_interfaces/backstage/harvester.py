# -*- coding: utf-8 -*-

# http://backstage-docs.openstate.eu/

# for generic docs,
# there are interfaces, the technical stuff, the api's, represented by modules
# a specific interface can be accessed, using a configuration
# within an interface, there can be collections
# collections have records

import common
from mangrove_libs.interface import Interface
from formatter.nllom import makeLOM, getEmptyLomDict
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
from time import sleep
from datetime import datetime


class Harvester(Interface):
	"""backstage harvester"""

	def __init__(self,config):
		Interface.__init__(self, config)
		Interface.handleRequestsProxy(self)


	def harvest(self,collection=""):
		self.logger.info("Starting harvesting")
		fromdate = datetime.fromtimestamp(int(self.DB.collection_updated)).strftime('%Y-%m-%d')

		self.getPage(fromdate)
		self.DB.touchCollection(self.startts)


	def getPage(self,fromdate,token=0):
		result = common.getResultPage(self.httpProxy,self.config["configuration"],fromdate,token)

		for video_id in result["videos"].keys():
			v = result["videos"][video_id]
			r = self.getDefaultRecord()
			# title might also be a combination of program title, title and subtitle, check that later
			r["original_id"] = video_id
			r["title"] = v["item"]["_source"]["title"]
			r["location"] = v["item"]["_source"]["meta"]["original_object_urls"]["html"]
			for author in v["metadata"]["authors"]:
				r["author"].append({ "fn": author})
			if "description" in v["item"]["_source"]:
				r["description"] = v["item"]["_source"]["description"]
			if "tags" in v["item"]["_source"]:
				r["keywords"] = v["item"]["_source"]["tags"]
			if "ageGroups" in v["item"]["_source"]:
				r["typicalagerange"] = v["item"]["_source"]["ageGroups"]
			if "tijdsduur" in v["metadata"]:
				r["duration"] = common.getDuration(v["metadata"]["tijdsduur"] )

			lom = makeLOM(r)
			oaidc = makeOAIDC(self.getOaidcRecord(r))
			self.storeResult(r,self.config["configuration"],lom,oaidc)

		if result["meta"]["token"] < result["meta"]["total"]:
			self.logger.debug( "token = " + str(result["meta"]["token"]) + ", total = " + str(result["meta"]["total"]) )
			sleep(5)
			self.getPage(fromdate,result["meta"]["token"])


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
