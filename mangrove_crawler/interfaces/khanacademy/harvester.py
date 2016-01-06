# -*- coding: utf-8 -*-
from formatter.nllom import makeLOM, getEmptyLomDict, formatDurationFromSeconds
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
from formatter.skos import makeSKOS
from mangrove_crawler.common import getRequestsProxy, getLogger, getTimestampFromZuluDT, downloadFile
import MySQLdb
import MySQLdb.cursors
import json
from rdflib.graph import Graph
from rdflib import URIRef
from rdflib.namespace import SKOS
from time import time
from uuid import uuid4


class Harvester:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1,cursorclass=MySQLdb.cursors.DictCursor)
		self.DB.set_character_set('utf8')
		self.httpProxy=None
		self.logger = getLogger('khan academy harvester')
		self.khanhost = "http://www.khanacademy.org/"
		self.currenttopic = ""
		self.topicscheme = { "identifier": self.khanhost + "library", "topconcepts": set() }
		self.totaltopics = dict()
		# maybe this should be in some kind of config
		self.topicwhitelist = ["computing", "math", "science", "economics-finance-domain", "humanities" ]

		self.mappinggraph = Graph()
		self.mappinggraph.parse(self.config["work_dir"] + "/khanmapping.rdf", format="xml")
		
		self.obkgraph = Graph()
		self.obkgraph.parse(self.config["work_dir"] + "/obk-skos.xml", format="xml")

		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getRequestsProxy(self.config["proxy_host"],self.config["proxy_port"])


	def harvest(self,part=""):
		""" download the topic tree """
		self.logger.info("Downloading topictree")
		downloadFile(self.httpProxy,"http://www.khanacademy.org/api/v1/topictree",self.config["work_dir"] + "/topictree.json")
		f = open(self.config["work_dir"] + "/topictree.json", 'r')
		result = json.loads(f.read() )
		f.close()
		
		""" check if dataset is updated """
		c = self.DB.cursor()
		query = "SELECT * FROM collections WHERE configuration = \"" + self.config["configuration"] + "\""
		c.execute(query)
		row = c.fetchone()
		self.config["collection_id"] = row["id"]
		
		if row["updated"] > getTimestampFromZuluDT(result["creation_date"]):
			self.logger.info("upstream data has not been updated, exiting")
			quit()
		
		""" parse all the data """
		for node in result["children"]:
			if node["slug"] in self.topicwhitelist:
				if part == "topics":
					self.parseNodeTopic(node)
					print(makeSKOS(self.topicscheme,self.totaltopics))
				else:
					self.parseNodeContent(node)

		""" update the collection updated timestamp """
		timestamp = int(time())
		query = "UPDATE collections SET updated=%s WHERE id=%s"
		c.execute(query,(timestamp,row["id"]))
		self.DB.commit()


	def parseNodeContent(self,node):
		if node["kind"] == "Topic":
			self.currenttopic = self.normalizeKhanTopicId(node["ka_url"])
		
		if node["kind"] == "Video":
			r = self.getDefaultKhanRecord()
			r["original_id"] = node["translated_youtube_id"]
			r["title"] = node["title"]
			r["description"] = node["description"]
			if node["keywords"]:
				r["keywords"] = node["keywords"].split(", ")
			if node["date_added"]:
				r["publishdate"] = node["date_added"]
			r["duration"] = formatDurationFromSeconds(node["duration"])
			r["format"] = "video/x-flv"
			r["location"] = "http://youtu.be/" + node["translated_youtube_id"]
			r["learningresourcetype"] = "informatiebron"
			r["intendedenduserrole"] = "learner"
			r["copyright"] = node["ka_user_license"] + "-40"
			r["thumbnail"] = node["image_url"]
			r["identifier"] = [ 
				{ "catalog": "Youtube", "value": node["translated_youtube_id"] }, 
				{ "catalog": "URI", "value": node["ka_url"] }, 
				{ "catalog": "URI", "value": "http://youtu.be/" + node["translated_youtube_id"] } ]
			r["author"] = node["author_names"]
			
			
			for o in self.mappinggraph.objects(URIRef(self.currenttopic), SKOS.closeMatch):
				r["discipline"].append( [ self.findTaxons(list(),o) ] )
			
			self.storeResults(r,"video")

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
		r = getEmptyLomDict()
		r["publisher"] = "Khan Academy"
		r["cost"] = "no"
		r["language"] = "en"
		r["aggregationlevel"] = "2"
		r["metalanguage"] = "en"
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


	def normalizeKhanTopicId(self,url):
		return url.replace(" ","-")


	def findTaxons(self,taxonpath,obkid):
		# checks if id is root, or id has multiple parents,
		# in both cases, add the id, and break
		if len(list(self.obkgraph.objects(URIRef(obkid), SKOS.broader))) != 1:
			taxonpath.append( { "id": obkid, "value": str(self.obkgraph.value( URIRef(obkid), SKOS.prefLabel) ) } )
			return taxonpath
		# only when an id has one parent, continue with the function
		# for the parent id
		elif len(list(self.obkgraph.objects(URIRef(obkid), SKOS.broader))) == 1:
			taxonpath.append( { "id": obkid, "value": str(self.obkgraph.value( URIRef(obkid), SKOS.prefLabel) ) } )
			return self.findTaxons(taxonpath, str(self.obkgraph.value( URIRef(obkid), SKOS.broader)) )


	def storeResults(self,record,setspec):
		lom = makeLOM(record)
		oaidc = makeOAIDC(self.getOaidcRecord(record))
		timestamp = int(time())
		c = self.DB.cursor()

		""" retrieve by page_id, if exists, update, else insert """
		query = "SELECT updated FROM oairecords WHERE original_id = \"" + record["original_id"] + "\""
		c.execute(query)
		row = c.fetchone()
		
		if row:
			""" update only if actually new """
			if getTimestampFromZuluDT(record["publishdate"]) > row["updated"]:
				query = "UPDATE oairecords SET updated=%s, lom=%s, oaidc=%s WHERE original_id=%s"
				c.execute(query,(timestamp,lom,oaidc,record["original_id"]))
		else:
			identifier = uuid4()
			query = "INSERT INTO oairecords (identifier,original_id,collection_id,setspec,updated,lom,oaidc) VALUES ( %s, %s, %s, %s, %s, %s, %s )"
			c.execute(query, (identifier,record["original_id"],self.config["collection_id"],setspec,timestamp,lom,oaidc))

		self.DB.commit()
