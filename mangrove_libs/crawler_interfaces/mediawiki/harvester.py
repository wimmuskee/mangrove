# -*- coding: utf-8 -*-

from mangrove_libs.textprocessing import TextProcessor
from mangrove_libs.common import downloadFile, checkLocal, checkPrograms, getTimestampFromZuluDT
from mangrove_libs.interface import Interface
from formatter.nllom import makeLOM, getEmptyLomDict, formatDurationFromSeconds
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
import re
from subprocess import call
from os import path
from lxml import etree
import mwparserfromhell
import datetime

class Harvester(Interface):
	"""mediawiki harvester"""

	def __init__(self,config,testing=False):
		Interface.__init__(self, config)
		Interface.handleRequestsProxy(self)
		self.re_htmltags = re.compile('<[^<]+?>')
		self.ns = {'mw': 'http://www.mediawiki.org/xml/export-0.10/'}
		self.record = {}

		# if True, execute some pass-through pipeline functions separately
		self.testing = testing

		if checkLocal:
			self.share_prefix = "share/interfaces/mediawiki/"
		else:
			self.share_prefix = "/usr/share/mangrove/interfaces/mediawiki/"

		checkPrograms(["gunzip", "bunzip2", "mysql"])


	def harvest(self,part=""):
		""" Wrapper for the harvestproces, individual record store is called in readXmlExport(). """
		self.getData()
		self.readXmlExport()
		self.cleanup()


	def getData(self):
		""" Downloading and unpacking the files.
		Unpacking at shell level, Wikipedia files too large for Python in-memory processing.
		"""
		# temporary fix
		if self.config["wiki"] == "wikikids":
			src_prefix = self.config["download_path"]
		else:
			src_prefix = self.config["download_path"] + self.config["wiki"] + "-"

		self.logger.info("Downloading page xml file")
		page_xml_bz = self.FS.workdir + "/pages-articles.xml.bz2"
		downloadFile(self.httpProxy, src_prefix + "latest-pages-articles.xml.bz2", page_xml_bz)

		self.logger.info("Unpacking page xml file")
		if path.isfile(page_xml_bz):
			call("bunzip2 " + page_xml_bz, shell=True)

		self.logger.info("Removing downloaded files")
		self.FS.removeFile(page_xml_bz)

	def readXmlExport(self):
		""" Reads the downloaded article xml, and extracts the required information.
		The parsing occurs iterated, so all extracted information is stored in the same process."""
		for event, elem in etree.iterparse(self.FS.workdir + "/pages-articles.xml", events=('start', 'end', 'start-ns', 'end-ns')):
			if event == "start":
				if elem.tag == "{" + self.ns["mw"] + "}page":
					self.recordmeta = { "title": None, "revision_id": None, "timestamp": None }
					self.recordtext = ""

					wiki_ns = self.__getXpathValue(elem,"mw:ns/text()")
					bytesize = self.__getXpathValue(elem,"mw:revision/mw:text/@bytes")

					if wiki_ns != "0" or not bytesize:
						continue
					if int(bytesize) < 2000:
						continue

					self.recordmeta["title"] = self.__getXpathValue(elem,"mw:title/text()")
					self.recordmeta["revision_id"] = self.__getXpathValue(elem,"mw:revision/mw:id/text()")
					self.recordmeta["timestamp"] = self.__getXpathValue(elem,"mw:revision/mw:timestamp/text()")

					meta_complete = True
					for key in self.recordmeta:
						if not self.recordmeta[key]:
							meta_complete = False

					if not meta_complete or not self.__checkListOf() or not self.__checkUpdate():
						continue

					wiki_text = self.__getXpathValue(elem,"mw:revision/mw:text/text()")
					if wiki_text:
						wikicode = mwparserfromhell.parse(wiki_text)
						self.recordtext = wikicode.strip_code(True, True)

					if self.recordtext and not self.testing:
						self.setData()

				elem.clear()

	def setData(self):
		""" Basically gets all data with some helper functions, and stores it. """
		self.__setDefaultMediawikiRecord()
		url_title = self.recordmeta["title"].replace(" ","_")
		lines = self.recordtext.split('\n')

		self.record["title"] = self.recordmeta["title"]
		self.record["description"] = self.re_htmltags.sub("",lines[0])
		self.record["publisher"] = self.config["wiki"]
		self.record["identifier"] = [ { "catalog": "URI", "value": self.config["host"] + url_title } ]
		self.record["version"] = self.recordmeta["timestamp"][:10].replace("-","")
		self.record["publishdate"] = self.recordmeta["timestamp"]
		self.record["location"] = self.config["host"] + url_title
		self.record["isversionof"] = self.config["host"] + "index.php?title=" + url_title + "&oldid=" + self.recordmeta["revision_id"]

		textproc = TextProcessor(self.recordtext,'nl_NL')
		self.record["typicalagerange"] = str(textproc.calculator.min_age) + "+"
		self.record["typicallearningtime"] = formatDurationFromSeconds(textproc.getReadingTime(textproc.calculator.scores['word_count'],textproc.calculator.min_age))

		for kw in textproc.getKeywords():
			self.record["keywords"].append(kw)

		contexts = []
		if self.config["context_static"]:
			contexts = self.config["context_static"].split("|")
		if self.config["context_dynamic"]:
			if textproc.calculator.min_age < 13:
				contexts.append("PO")
			elif textproc.calculator.min_age > 12 and textproc.calculator.min_age < 19:
				contexts.append("VO")
		self.record["context"] = list(set(contexts))

		## override some if config
		if self.config["age_range"]:
			self.record["typicalagerange"] = self.config["age_range"]
			min_age = re.search(r'^\d+', self.config["age_range"]).group(0)
			self.record["typicallearningtime"] = formatDurationFromSeconds(textproc.getReadingTime(textproc.calculator.scores['word_count'],min_age))

		lom = makeLOM(self.record)
		oaidc = makeOAIDC(self.__getOaidcRecord(self.record))

		## and store
		if not self.testing:
			self.storeResult({"original_id": url_title},None,lom,oaidc)

	def cleanup(self):
		self.logger.info("Cleaning up workdir files")
		self.FS.removeFile(self.FS.workdir + "/pages-articles.xml")


	def __getXpathValue(self,element,xpath):
		value = element.xpath(xpath, namespaces=self.ns)
		if value:
			return value[0]
		else:
			return None

	def __checkUpdate(self):
		""" Checks if record timestamp is higher than one in oairecords."""
		url_title = self.recordmeta["title"].replace(" ","_")
		record_updated = getTimestampFromZuluDT(self.recordmeta["timestamp"])

		oairecord = self.DB.getRecordByOriginalId(url_title)
		if oairecord:
			if record_updated > int(oairecord["updated"]):
				return True
			else:
				return False
		else:
			return True

	def __checkListOf(self):
		""" We don't want listpages, check if title starts with defined prefix."""
		prefixlen = len(self.config["list_prefix"])
		if self.recordmeta["title"][:prefixlen] == self.config["list_prefix"]:
			return False
		return True

	def __setDefaultMediawikiRecord(self):
		r = getEmptyLomDict()
		r["cost"] = "no"
		r["language"] = "nl"
		r["aggregationlevel"] = "2"
		r["metalanguage"] = "nl"
		r["structure"] = "hierarchical"
		r["format"] = "text/html"
		r["intendedenduserrole"] = "learner"
		r["learningresourcetype"] = "informatiebron"
		r["interactivitytype"] = "expositive"
		r["copyrightandotherrestrictions"] = "cc-by-sa-30"
		self.record = r

	def __getOaidcRecord(self,record):
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
