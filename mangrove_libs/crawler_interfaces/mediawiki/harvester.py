# -*- coding: utf-8 -*-

from mangrove_libs.textprocessing import TextProcessor
from mangrove_libs.common import downloadFile, checkLocal, checkPrograms, getTimestampFromZuluDT
from mangrove_libs.interface import Interface
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
import re
from subprocess import call
from os import path
from lxml import etree
import mwparserfromhell
import datetime
from pylom.writer import LomWriter

class Harvester(Interface):
	"""mediawiki harvester"""

	def __init__(self,config,testing=False):
		Interface.__init__(self, config)
		Interface.handleRequestsProxy(self)
		Interface.setLomVocabSources(self)
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
		revision_url = str(self.config["host"] + "index.php?title=" + url_title + "&oldid=" + self.recordmeta["revision_id"])
		lines = self.recordtext.split('\n')

		self.record["description"] = self.re_htmltags.sub("",lines[0])
		self.record["identifier"] = [ { "catalog": "URI", "value": self.config["host"] + url_title } ]
		self.record["location"] = self.config["host"] + url_title
		self.record["relation"] = [{ 
			"kind": "isversionof", 
			"resource": { "description": "De metadata is op deze revisie gebaseerd.", "catalogentry": [ {"catalog": "URI", "entry": revision_url } ] }
		}]

		textproc = TextProcessor(self.recordtext,'nl_NL')
		self.record["educational"][0]["typicalagerange"] = str(textproc.calculator.min_age) + "+"
		self.record["educational"][0]["typicallearningtime"] = "PT" + str(textproc.getReadingTime(textproc.calculator.scores['word_count'],textproc.calculator.min_age)) + "S"

		for kw in textproc.getKeywords():
			self.record["keyword"].append(kw)

		contexts = []
		if self.config["context_static"]:
			contexts = self.config["context_static"].split("|")
		if self.config["context_dynamic"]:
			if textproc.calculator.min_age < 13:
				contexts.append("PO")
			elif textproc.calculator.min_age > 12 and textproc.calculator.min_age < 19:
				contexts.append("VO")

		self.record["educational"][0]["context"] = list(set(contexts))

		## override some if config
		if self.config["age_range"]:
			self.record["educational"][0]["typicalagerange"] = self.config["age_range"]
			min_age = re.search(r'^\d+', self.config["age_range"]).group(0)
			self.record["educational"][0]["typicallearningtime"] = "PT" + str(textproc.getReadingTime(textproc.calculator.scores['word_count'],min_age)) + "S"

		lomwriter = LomWriter("nl")
		lomwriter.vocabulary_sources.update(self.vocab_sources)
		lomwriter.parseDict(self.record)
		oaidc = makeOAIDC(self.__getOaidcRecord(self.record))

		## and store
		if not self.testing:
			self.storeResult({"original_id": url_title},None,lomwriter.lom,oaidc)

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
		r = {}
		r["title"] = self.recordmeta["title"]
		r["keyword"] = []
		r["cost"] = "no"
		r["language"] = "nl"
		r["aggregationlevel"] = "2"
		r["metalanguage"] = "nl"
		r["version"] = self.recordmeta["timestamp"][:10].replace("-","")
		r["contribute"] = [{"role": "publisher", "entity": "BEGIN:VCARD\nFN:" + str(self.config["wiki"]) + "\nEND:VCARD", "date": self.recordmeta["timestamp"]}]
		r["metadatascheme"] = ["LOMv1.0","nl_lom_v1p0"]
		r["structure"] = "hierarchical"
		r["format"] = "text/html"
		r["educational"] = [{
			"interactivitytype": "expositive",
			"intendedenduserrole": "learner",
			"learningresourcetype": "informatiebron"
		}]
		r["copyrightandotherrestrictions"] = "cc-by-sa-30"
		self.record = r

	def __getOaidcRecord(self,record):
		r = getEmptyOaidcDict()
		r["title"] = record["title"]
		r["description"] = record["description"]
		r["subject"] = record["keyword"]
		r["publisher"] = self.config["wiki"]
		r["format"] = record["format"]
		r["identifier"] = record["location"]
		r["language"] = record["language"]
		r["rights"] = record["copyrightandotherrestrictions"]
		return r
