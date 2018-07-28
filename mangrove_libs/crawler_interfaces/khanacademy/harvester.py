# -*- coding: utf-8 -*-
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
from formatter.skos import makeSKOS
from mangrove_libs.interface import Interface
from mangrove_libs.common import getTimestampFromZuluDT, downloadFile
import json
from rdflib.graph import Graph
from rdflib import URIRef
from rdflib.namespace import SKOS
from pylom.writer import LomWriter

# note:
# to fully refresh this collection, because for instance you have a new mapping
# you have to reset the collection timestamp, but also the record timestamps

class Harvester(Interface):
	"""khan academy harvester"""
	
	def __init__(self,config):
		Interface.__init__(self, config)
		Interface.handleRequestsProxy(self)
		Interface.setLomVocabSources(self)

		self.khanhost = "http://www.khanacademy.org/"
		self.currenttopic = ""
		self.topicscheme = { "identifier": self.khanhost + "library", "topconcepts": set() }
		self.totaltopics = dict()
		# maybe this should be in some kind of config
		self.topicwhitelist = ["computing", "math", "science", "economics-finance-domain", "humanities" ]

		self.mappinggraph = Graph()
		self.mappinggraph.parse(self.FS.workdir + "/khanmapping.rdf", format="xml")
		
		self.obkgraph = Graph()
		self.obkgraph.parse(self.FS.workdir + "/obk-skos.xml", format="xml")


	def harvest(self,part=""):
		""" download the topic tree """
		self.logger.info("Downloading topictree")
		downloadFile(self.httpProxy,"http://www.khanacademy.org/api/v1/topictree",self.FS.workdir + "/topictree.json")
		with open(self.FS.workdir + "/topictree.json", 'r') as f:
			result = json.loads(f.read())

		""" check if dataset is updated """
		if self.DB.collection_updated > getTimestampFromZuluDT(result["creation_date"]):
			self.logger.info("upstream data has not been updated, exiting")
			exit()
		
		""" parse all the data """
		for node in result["children"]:
			if node["slug"] in self.topicwhitelist:
				if part == "topics":
					self.parseNodeTopic(node)
					print(makeSKOS(self.topicscheme,self.totaltopics))
				else:
					self.parseNodeContent(node)

		""" update the collection updated timestamp """
		self.DB.touchCollection(self.startts)


	def parseNodeContent(self,node):
		# ensure record published ts for check
		published_ts = 0
		if node["date_added"]:
			published_ts = getTimestampFromZuluDT(node["date_added"])

		if node["kind"] == "Topic":
			self.currenttopic = self.normalizeKhanTopicId(node["ka_url"])

		# if video, and published ts is newer, parse and store
		if node["kind"] == "Video" and published_ts > self.DB.collection_updated:
			r = self.getDefaultKhanRecord()
			r["title"] = node["title"]
			r["description"] = node["description"]
			if node["keywords"]:
				r["keyword"] = node["keywords"].split(", ")
			if node["date_added"]:
				r["publishdate"] = node["date_added"]
			r["duration"] = "PT" + str(node["duration"]) + "S"
			r["format"] = "video/x-flv"
			r["location"] = "http://youtu.be/" + node["translated_youtube_id"]
			r["educational"][0]"learningresourcetype"] = "informatiebron"
			r["educational"][0]["intendedenduserrole"] = "learner"
			r["copyrightandotherrestrictions"] = node["ka_user_license"] + "-40"
			r["relation"] = [{"kind": "thumbnail", "resource": { "catalogentry": [ {"catalog": "URI", "entry": str(node["image_url"]) } ] } }]
			r["identifier"] = [ 
				{ "catalog": "Youtube", "value": str(node["translated_youtube_id"]) }, 
				{ "catalog": "URI", "value": str(node["ka_url"]) }, 
				{ "catalog": "URI", "value": "http://youtu.be/" + str(node["translated_youtube_id"]) } ]

			r["contribute"] = [] 
			if node["date_added"]:
				r["contribute"].append({"role": "publisher", "entity": "BEGIN:VCARD\nFN:Khan Academy\nEND:VCARD", "date": str(node["date_added"])})
			else:
				r["contribute"].append({"role": "publisher", "entity": "BEGIN:VCARD\nFN:Khan Academy\nEND:VCARD"})

			for author in node["author_names"]:
				r["contribute"].append({"role": "author", "entity": "BEGIN:VCARD\nFN:" + str(author) + "\nEND:VCARD"})

			discipline_taxonpaths = []
			for o in self.mappinggraph.objects(URIRef(self.currenttopic), SKOS.closeMatch):
				discipline_taxonpaths.append( { "source": "http://purl.edustandaard.nl/begrippenkader/", "taxon": self.findTaxons(list(),o) )

			for o in self.mappinggraph.objects(URIRef(self.currenttopic), SKOS.broadMatch):
				discipline_taxonpaths.append( { "source": "http://purl.edustandaard.nl/begrippenkader/", "taxon": self.findTaxons(list(),o) )

			if discipline_taxonpaths:
				r["classification"] = [ { "purpose": "discipline", "taxonpath": discipline_taxonpaths } ]

			lomwriter = LomWriter("en")
			lomwriter.vocabulary_sources.update(self.vocab_sources)
			lomwriter.parseDict(r)
			oaidc = makeOAIDC(self.getOaidcRecord(r))
			self.storeResults({"original_id": node["translated_youtube_id"],"video",lomwriter.lom,oaidc)

		if "children" in node:
			for cnode in node["children"]:
				self.parseNodeContent(cnode)


	def parseNodeTopic(self,node):
		if node["kind"] == "Topic":
			identifier = self.normalizeKhanTopicId(node["extended_slug"])
			#print(identifier)
			topic = dict()
			topic["label"] = node["title"]
			topic["about"] = self.khanhost + identifier
			topic["broader"] = set()
			topic["narrower"] = set()
			self.totaltopics[identifier] = topic
			
			topicpath = identifier.split("/")
			if len(topicpath) == 1:
				self.topicscheme["topconcepts"].update([ self.khanhost + topicpath[0] ])
			
			if len(topicpath) > 1:
				broadertopic =  "/".join(topicpath[:-1])
				self.totaltopics[identifier]["broader"].update([ self.khanhost + broadertopic])
				self.totaltopics[broadertopic]["narrower"].update([ self.khanhost + identifier]) 


		if "children" in node:
			for cnode in node["children"]:
				self.parseNodeTopic(cnode)


	def getDefaultKhanRecord(self):
		r = {}
		r["cost"] = "no"
		r["language"] = "en"
		r["aggregationlevel"] = "2"
		r["metalanguage"] = "en"
		return r


	def getOaidcRecord(self,record):
		r = getEmptyOaidcDict()
		r["title"] = record["title"]
		r["description"] = record["description"]
		r["subject"] = record["keyword"]
		r["publisher"] = "Khan Academy"
		r["format"] = record["format"]
		r["identifier"] = record["location"]
		r["language"] = record["language"]
		r["rights"] = record["copyrightandotherrestrictions"]
		return r


	def normalizeKhanTopicId(self,url):
		return url.replace(" ","-")


	def findTaxons(self,taxonpath,obkid):
		# checks if id is root, or id has multiple parents,
		# in both cases, add the id, and break
		if len(list(self.obkgraph.objects(URIRef(obkid), SKOS.broader))) != 1:
			taxonpath.append( { "id": obkid[43:], "entry": str(self.obkgraph.value( URIRef(obkid), SKOS.prefLabel) ) } )
			return taxonpath
		# only when an id has one parent, continue with the function
		# for the parent id
		elif len(list(self.obkgraph.objects(URIRef(obkid), SKOS.broader))) == 1:
			taxonpath.append( { "id": obkid[43:], "entry": str(self.obkgraph.value( URIRef(obkid), SKOS.prefLabel) ) } )
			return self.findTaxons(taxonpath, str(self.obkgraph.value( URIRef(obkid), SKOS.broader)) )
