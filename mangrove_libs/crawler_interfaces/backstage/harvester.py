# -*- coding: utf-8 -*-

# http://backstage-docs.openstate.eu/

# for generic docs,
# there are interfaces, the technical stuff, the api's, represented by modules
# a specific interface can be accessed, using a configuration
# within an interface, there can be collections
# collections have records

import common
from mangrove_libs.interface import Interface
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
from time import sleep
from datetime import datetime
from pylom.writer import LomWriter

class Harvester(Interface):
	"""backstage harvester"""

	def __init__(self,config):
		Interface.__init__(self, config)
		Interface.handleRequestsProxy(self)
		Interface.setLomVocabSources(self)
		self.record = {}

	def harvest(self,collection=""):
		self.logger.info("Starting harvesting")
		fromdate = datetime.fromtimestamp(int(self.DB.collection_updated)).strftime('%Y-%m-%d')

		self.getPage(fromdate)
		self.DB.touchCollection(self.startts)


	def getPage(self,fromdate,token=0):
		result = common.getResultPage(self.httpProxy,self.config["configuration"],fromdate,token)

		for video_id in result["videos"].keys():
			v = result["videos"][video_id]
			self.__setDefaultRecord()
			# title might also be a combination of program title, title and subtitle, check that later
			self.record["title"] = v["item"]["_source"]["title"]
			r["location"] = v["item"]["_source"]["meta"]["original_object_urls"]["html"]
			for author in v["metadata"]["authors"]:
				self.record["contribute"].append( [{"role": "author", "entity": "BEGIN:VCARD\nFN:" + str(author) + "\nEND:VCARD"}]
			if "description" in v["item"]["_source"]:
				self.record["description"] = v["item"]["_source"]["description"]
			if "tags" in v["item"]["_source"]:
				self.record["keyword"] = v["item"]["_source"]["tags"]
			if "ageGroups" in v["item"]["_source"]:
				self.record["educational"][0]["typicalagerange"] = v["item"]["_source"]["ageGroups"]
			if "tijdsduur" in v["metadata"]:
				self.record["educational"][0]["duration"] = common.getDuration(v["metadata"]["tijdsduur"] )

			lomwriter = LomWriter("nl")
			lomwriter.vocabulary_sources.update(self.vocab_sources)
			lomwriter.parseDict(self.record)
			oaidc = makeOAIDC(self.getOaidcRecord(r))
			self.storeResult({"original_id": video_id},self.config["configuration"],lomwriter.lom,oaidc)

		if result["meta"]["token"] < result["meta"]["total"]:
			self.logger.debug( "token = " + str(result["meta"]["token"]) + ", total = " + str(result["meta"]["total"]) )
			sleep(5)
			self.getPage(fromdate,result["meta"]["token"])


	def __setDefaultRecord(self):
		r = {}
		r["contribute"] = [{"role": "publisher", "entity": "BEGIN:VCARD\nFN:" + str(self.config["publisher"]) + "\nEND:VCARD"}]
		r["cost"] = "no"
		r["language"] = "nl"
		r["aggregationlevel"] = "2"
		r["educational"] = [{
			"intendedenduserrole": "learner",
			"learningresourcetype": "informatiebron"
		}]
		self.record = r


	def getOaidcRecord(self,record):
		r = getEmptyOaidcDict()
		r["title"] = record["title"]
		r["description"] = record["description"]
		r["subject"] = record["keyword"]
		r["publisher"] = self.config["publisher"]
		r["identifier"] = record["location"]
		r["language"] = record["language"]
		return r
