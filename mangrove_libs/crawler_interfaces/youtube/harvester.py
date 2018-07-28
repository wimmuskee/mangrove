# -*- coding: utf-8 -*-
from mangrove_libs.crawler_interfaces.youtube import common
from mangrove_libs.interface import Interface
import json
from time import sleep
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
from pylom.writer import LomWriter


class Harvester(Interface):
	"""youtube harvester"""

	def __init__(self,config,testing=False):
		Interface.__init__(self, config)
		Interface.handleHttplib2Proxy(self)
		Interface.setLomVocabSources(self)
		self.page = {}
		self.record = {}

		# if True, execute some pass-through pipeline functions separately
		self.testing = testing


	def harvest(self,part=None):
		self.logger.info("Harvesting all channels")
		f = open("youtubechannels.json", 'r')
		channels = json.loads(f.read() )
		f.close()
		
		for channel in channels:
			self.setChannelPage(channel,"")
		
		self.DB.touchCollection(self.startts)


	def setChannelPage(self,channel,token):
		self.page = common.getChannelPage(self.httpProxy,self.config["developer_key"],channel["youtube_id"],self.DB.collection_updated,token)
		self.setData(channel)

		if self.page["meta"]["token"]:
			sleep(5)
			self.setChannelPage(channel,self.page["meta"]["token"])


	def setData(self,channel):
		for vid in self.page["videos"].keys():
			self.recordmeta = self.page["videos"][vid]

			self.__setDefaultLomRecord()
			self.record["metalanguage"] = channel["language"]
			self.record["language"] = channel["language"]
			self.record["keyword"] = channel["keyword"]
			self.record["contribute"].append({"role": "author", "entity": "BEGIN:VCARD\nFN:" + channel["title"] + "\nURL:" + "http://www.youtube.com/user/" + channel["username"] + "\nEND:VCARD"})

			self.record["educational"] = [{
				"context": channel["context"],
				"learningresourcetype": channel["learningresourcetype"],
				"intendedenduserrole": channel["intendedenduserrole"] 
			}]

			if self.recordmeta["license"] == "creativeCommon":
				self.record["copyrightandotherrestrictions"] = "cc-by-30"
			elif self.recordmeta["license"] == "youtube":
				self.record["copyrightandotherrestrictions"] = "yes"
				self.record["copyrightdescription"] = "Standaard YouTube licentie: http://www.youtube.com/t/terms"

			self.record["relation"] = [{"kind": "thumbnail", "resource": { "catalogentry": [ {"catalog": "URI", "entry": str(self.recordmeta["thumbnail"]) } ] } }]
			if self.recordmeta["embed"]:
				self.record["relation"].append({"kind": "embed", "resource": { "catalogentry": [ {"catalog": "URI", "entry": str("http://www.youtube.com/embed/" + self.recordmeta["youtube_id"]) } ] } })

			if channel["discipline"]:
				self.record["classification"] = [{
					"purpose": "discipline",
					"taxonpath": [ { "source": "http://purl.edustandaard.nl/begrippenkader/", "taxon": [
                    { "id": channel["discipline"][0]["id"], "entry": channel["discipline"][0]["value"] } ] } ] } ]

			lomwriter = LomWriter(channel["language"])
			lomwriter.vocabulary_sources.update(self.vocab_sources)
			lomwriter.parseDict(self.record)
			oaidc = makeOAIDC(self.getOaidcRecord(self.record))

			if not self.testing:
				self.storeResult({"original_id": video["youtube_id"]},channel["setspec"],lomwriter.lom,oaidc)


	def __setDefaultLomRecord(self):
		r = {}
		r["identifier"] = [
			{ "catalog": "YouTube", "value": self.recordmeta["youtube_id"] },
			{ "catalog": "URI", "value": "http://youtu.be/" + self.recordmeta["youtube_id"] } ]
		r["title"] = self.recordmeta["title"]
		r["description"] = self.recordmeta["description"]
		r["contribute"] = [ {"role": "publisher", "entity": "BEGIN:VCARD\nFN:YouTube\nEND:VCARD", "date": str(self.recordmeta["publishdate"])} ]
		r["location"] = "http://youtu.be/" + self.recordmeta["youtube_id"]
		r["duration"] = "PT" + str(self.recordmeta["duration"]) + "S"
		r["cost"] = "no"
		r["format"] = "video/x-flv"
		r["aggregationlevel"] = "2"
		self.record = r


	def getOaidcRecord(self,record):
		r = getEmptyOaidcDict()
		r["title"] = self.record["title"]
		r["description"] = self.record["description"]
		r["subject"] = self.record["keyword"]
		r["publisher"] = "YouTube"
		r["format"] = self.record["format"]
		r["identifier"] = self.record["location"]
		r["language"] = self.record["language"]
		if self.record["copyrightandotherrestrictions"] == "yes":
			r["rights"] = self.record["copyrightdescription"]
		else:
			r["rights"] = self.record["copyrightandotherrestrictions"]
		return r
