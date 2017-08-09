# -*- coding: utf-8 -*-

import common
from mangrove_libs.textprocessing import TextProcessor
from mangrove_libs.common import downloadFile, checkLocal, checkPrograms
from mangrove_libs.interface import Interface
from formatter.nllom import makeLOM, getEmptyLomDict, formatDurationFromSeconds
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
import re
from bz2 import BZ2File
from subprocess import Popen, PIPE, call
from os import walk, path, listdir
import datetime

class Harvester(Interface):
	"""mediawiki harvester"""

	def __init__(self,config):
		Interface.__init__(self, config)
		Interface.handleRequestsProxy(self)
		self.re_docid = re.compile(r'id="([0-9]*?)"')
		self.re_htmltags = re.compile('<[^<]+?>')
		self.pages_to_update = {}

		if checkLocal:
			self.share_prefix = "share/interfaces/mediawiki/"
		else:
			self.share_prefix = "/usr/share/mangrove/interfaces/mediawiki/"

		checkPrograms(["gunzip", "bunzip2", "WikiExtractor.py", "mysql"])


	def harvest(self,part=""):
		""" Getting the data in 4 steps, parsing is distributed in parseExtracts """
		self.getData()
		self.importData()
		self.getRecordsToUpdate()
		self.preprocessText()
		self.parseExtracts()
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

		self.logger.info("Downloading page sql file")
		page_sql_gz = self.FS.workdir + "/page.sql.gz"
		downloadFile(self.httpProxy, src_prefix + "latest-page.sql.gz", page_sql_gz)

		self.logger.info("Unpacking page sql file")
		if path.isfile(page_sql_gz):
			call("gunzip " + page_sql_gz, shell=True)

		self.logger.info("Downloading page xml file")
		page_xml_bz = self.FS.workdir + "/pages-articles.xml.bz2"
		downloadFile(self.httpProxy, src_prefix + "latest-pages-articles.xml.bz2", page_xml_bz)

		self.logger.info("Unpacking page xml file")
		if path.isfile(page_xml_bz):
			call("bunzip2 " + page_xml_bz, shell=True)

		self.logger.info("Downloading categories sql file")
		categories_sql_gz = self.FS.workdir + "/categorylinks.sql.gz"
		downloadFile(self.httpProxy, src_prefix + "latest-categorylinks.sql.gz", categories_sql_gz)
		
		self.logger.info("Unpacking categories sql file")
		if path.isfile(categories_sql_gz):
			call("gunzip " + categories_sql_gz, shell=True)

		self.logger.info("Removing downloaded files")
		self.FS.removeFile(page_sql_gz)
		self.FS.removeFile(page_xml_bz)
		self.FS.removeFile(categories_sql_gz)


	""" downloaded sql + custom sql to trim the total set """
	def importData(self):
		self.logger.info("Importing data in database")
		sqlfiles = [self.FS.workdir + "/page.sql", self.FS.workdir + "/categorylinks.sql"]
		sqlfiles.append(self.share_prefix + "importCategories.sql")
		sqlfiles.append(self.share_prefix + "importCategoryRelations.sql")
		sqlfiles.append(self.share_prefix + self.config["wiki"] + "_removeSmallPages.sql")
		sqlfiles.append(self.share_prefix + self.config["wiki"] + "_renameTables.sql")

		for sql in sqlfiles:
			process = Popen('mysql %s -u%s -p%s' % (self.config["db_name"], self.config["db_user"], self.config["db_passwd"]), stdout=PIPE, stdin=PIPE, shell=True)
			output = process.communicate(file(sql).read())


	def getRecordsToUpdate(self):
		""" Compare <source>_page table with oairecords table for this collection, and
		get the page_ids for the records that need to be updated."""
		c = self.DB.DB.cursor()
		c.execute("SELECT page_id AS id, page_title AS title, page_touched AS touched, page_latest AS lastrev_id FROM " + self.config["wiki"] + "_page")
		page_data = c.fetchall()
		c.close()

		for page in page_data:
			oairecord = self.DB.getRecordByOriginalId(page["title"])
			if oairecord:
				# page exists already in oai, find out if updated
				ts_touched = common.makeTimestamp(page["touched"])
				if ts_touched > int(oairecord["updated"]):
					self.pages_to_update[str(page["id"])] = { "touched": ts_touched, "title": page["title"], "lastrev_id": page["lastrev_id"] }
			else:
				self.pages_to_update[str(page["id"])] = { "touched": ts_touched, "title": page["title"], "lastrev_id": page["lastrev_id"] }


	def preprocessText(self):
		self.logger.info("Preprocessing text")
		outputdir = self.FS.workdir + "/extract"
		inputfile = self.FS.workdir + "/pages-articles.xml"
		script = self.share_prefix + "WikiExtractorWrapper.sh"
		call([script, inputfile, outputdir])


	def parseExtracts(self):
		self.logger.info("Parse text extracts")
		for (dirpath, dirnames, filenames) in walk(self.FS.workdir + "/extract"):
			for bzfile in filenames:
				self.parseExtract(dirpath + "/" + bzfile)


	def parseExtract(self,bzfile):
		with BZ2File(bzfile, 'r') as f:
			text = f.read()

		"""
		If line starts with <doc, make new article, fill it with lines,
		until the next </doc> is found, process the article, and start over.
		"""
		for line in text.split('\n'):
			if line[:4] == "<doc":
				article = ""
				id = self.re_docid.search(line).group(1)
			elif line[:6] == "</doc>":
				if id in self.pages_to_update:
					self.setData(self.pages_to_update[id],article)
			else:
				article += "\n" + line


	def setData(self,metadata,text):
		""" Basically gets all data with some helper functions, and stores it. """
		r = self.__getDefaultMediawikiRecord()
		r["publisher"] = self.config["wiki"]
		r["identifier"] = [ { "catalog": "URI", "value": self.config["host"] + metadata["title"] } ]
		r["version"] = datetime.datetime.fromtimestamp(metadata["touched"]).strftime('%d%m%Y')
		r["publishdate"] = datetime.datetime.fromtimestamp(metadata["touched"]).strftime('%Y-%m-%dT%H:%M:%SZ')
		r["location"] = self.config["host"] + metadata["title"]
		r["isversionof"] =  self.config["host"] + "/index.php?title=" + metadata["title"] + "&oldid=" + str(metadata["lastrev_id"])

		""" now fill with input """
		textproc = TextProcessor(text,'nl_NL')
		lines = textproc.text.split('\n')
		""" Not taking first line because before each line, a linebreak is inserted. """
		r["title"] = lines[1].decode('utf-8')
		r["typicalagerange"] = str(textproc.calculator.min_age) + "+"
		r["typicallearningtime"] = formatDurationFromSeconds(textproc.getReadingTime(textproc.calculator.scores['word_count'],textproc.calculator.min_age))

		keywords =  textproc.getKeywords()
		for kw in keywords:
			r["keywords"].append(kw.decode('utf-8'))

		if len(lines) >= 4:
			r["description"] = self.re_htmltags.sub("",lines[3]).decode('utf-8')

		contexts = []
		if self.config["context_static"]:
			contexts = self.config["context_static"].split("|")
		if self.config["context_dynamic"]:
			if textproc.calculator.min_age < 13:
				contexts.append("PO")
			elif textproc.calculator.min_age > 12 and textproc.calculator.min_age < 19:
				contexts.append("VO")
		r["context"] = list(set(contexts))

		# override some if config
		if self.config["age_range"]:
			r["typicalagerange"] = self.config["age_range"]
			min_age = re.search(r'^\d+', self.config["age_range"]).group(0)
			r["typicallearningtime"] = formatDurationFromSeconds(textproc.getReadingTime(textproc.calculator.scores['word_count'],min_age))

		lom = makeLOM(r)
		oaidc = makeOAIDC(self.__getOaidcRecord(r))

		# and store
		self.storeResult({"original_id": metadata["title"]},None,lom,oaidc)


	def cleanup(self):
		self.logger.info("Cleaning up workdir files")
		self.FS.removeFile(self.FS.workdir + "/categorylinks.sql")
		self.FS.removeFile(self.FS.workdir + "/page.sql")
		self.FS.removeFile(self.FS.workdir + "/pages-articles.xml")

		for subdir in listdir(self.FS.workdir + "/extract"):
			self.FS.removeDir(self.FS.workdir + "/extract/" + subdir)


	def __getDefaultMediawikiRecord(self):
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
		return r

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
