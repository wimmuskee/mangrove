# -*- coding: utf-8 -*-

import common
from mangrove_libs.textprocessing import TextProcessor
from mangrove_libs.common import downloadFile, checkLocal, getRequestsProxy, checkPrograms, getLogger
from storage.filesystem import Filesystem
import MySQLdb
import MySQLdb.cursors
import re
from bz2 import BZ2File
from subprocess import Popen, PIPE, call
from os import walk, path, listdir
from time import time
from uuid import uuid4

class Harvester:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1,cursorclass=MySQLdb.cursors.DictCursor)
		self.DB.set_character_set('utf8')
		self.FS = Filesystem(config)
		self.httpProxy = None
		self.config["dest_prefix"] = config["work_dir"] + "/" + config["wiki"] + "-"
		self.re_docid = re.compile(r'id="([0-9]*?)"')
		self.re_htmltags = re.compile('<[^<]+?>')
		self.logger = getLogger('mediawiki harvester')


		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getRequestsProxy(self.config["proxy_host"],self.config["proxy_port"])

		if checkLocal:
			self.share_prefix = "share/interfaces/mediawiki/"
		else:
			self.share_prefix = "/usr/share/mangrove/interfaces/mediawiki/"

		checkPrograms(["gunzip", "bunzip2", "WikiExtractor.py", "mysql"])


	def harvest(self,part=""):
		""" Getting the data in 4 steps, parsing is distributed in parseExtracts """
		self.getData()
		self.importData()
		self.preprocessText()
		self.parseExtracts()
		self.cleanup()


	def getData(self):
		""" Downloading and unpacking the files.
		Unpacking at shell level, Wikipedia files too large for Python in-memory processing.
		"""
		src_prefix = self.config["download_path"] + self.config["wiki"] + "-"

		self.logger.info("Downloading page sql file")
		downloadFile(self.httpProxy, src_prefix + "latest-page.sql.gz", self.config["dest_prefix"] + "page.sql.gz")
		
		self.logger.info("Unpacking page sql file")
		gzfile = self.config["dest_prefix"] + "page.sql.gz"
		if path.isfile(gzfile):
			call("gunzip " + gzfile, shell=True)

		self.logger.info("Downloading page xml file")
		downloadFile(self.httpProxy, src_prefix + "latest-pages-articles.xml.bz2", self.config["dest_prefix"] + "pages-articles.xml.bz2")
		
		self.logger.info("Unpacking page xml file")
		bzfile = self.config["dest_prefix"] + "pages-articles.xml.bz2"
		if path.isfile(bzfile):
			call("bunzip2 " + bzfile, shell=True)

		self.logger.info("Downloading categories sql file")
		downloadFile(self.httpProxy, src_prefix + "latest-categorylinks.sql.gz", self.config["dest_prefix"] + "categorylinks.sql.gz")
		
		self.logger.info("Unpacking categories sql file")
		gzfile = self.config["dest_prefix"] + "categorylinks.sql.gz"
		if path.isfile(gzfile):
			call("gunzip " + gzfile, shell=True)

		self.logger.info("Removing downloaded files")
		self.FS.removeFile(self.config["dest_prefix"] + "page.sql.gz")
		self.FS.removeFile(self.config["dest_prefix"] + "pages-articles.xml.bz2")
		self.FS.removeFile(self.config["dest_prefix"] + "categorylinks.sql.gz")


	""" downloaded sql + custom sql to trim the total set """
	def importData(self):
		self.logger.info("Importing data in database")
		sqlfiles = [self.config["dest_prefix"] + "page.sql", self.config["dest_prefix"] + "categorylinks.sql"]
		sqlfiles.append(self.share_prefix + "importCategories.sql")
		sqlfiles.append(self.share_prefix + "importCategoryRelations.sql")
		sqlfiles.append(self.share_prefix + self.config["wiki"] + "_removeSmallPages.sql")
		sqlfiles.append(self.share_prefix + self.config["wiki"] + "_selectTitles.sql")
		sqlfiles.append(self.share_prefix + self.config["wiki"] + "_renameTables.sql")

		for sql in sqlfiles:
			process = Popen('mysql %s -u%s -p%s' % (self.config["db_name"], self.config["db_user"], self.config["db_passwd"]), stdout=PIPE, stdin=PIPE, shell=True)
			output = process.communicate(file(sql).read())


	def preprocessText(self):
		self.logger.info("Preprocessing text")
		outputdir = self.config["work_dir"] + "/extract-" + self.config["wiki"]
		inputfile = self.config["dest_prefix"] + "pages-articles.xml"
		script = self.share_prefix + "WikiExtractorWrapper.sh"
		call([script, inputfile, outputdir])


	def parseExtracts(self):
		self.logger.info("Parse text extracts")
		for (dirpath, dirnames, filenames) in walk(self.config["work_dir"] + "/extract-" + self.config["wiki"]):
			for bzfile in filenames:
				self.parseExtract(dirpath + "/" + bzfile)


	def parseExtract(self,bzfile):
		c = self.DB.cursor()
		f = BZ2File(bzfile, 'r')
		text = f.read()
		f.close()

		"""
		If line starts with <doc, make new article, fill it with lines,
		until the next </doc> is found, process the article, and start over.
		"""
		for line in text.split('\n'):
			if line[:4] == "<doc":
				article = ""
				id = self.re_docid.search(line).group(1)
			elif line[:6] == "</doc>":
				query = "SELECT * FROM " + self.config["wiki"] + "_page WHERE page_id = " + id
				c.execute(query)
				row = c.fetchone()
				if row:
					self.setData(row,article)
			else:
				article += "\n" + line

		c.close()


	""" Basically gets all data with some helper functions """
	def setData(self,metadata,text):
		""" make clean object """
		self.page_id = 0
		self.urltitle = ""
		self.lastrev_id = 0
		self.title = ""
		self.description = ""
		self.keywords = []
		self.version = ""
		self.updated = 0
		self.min_age = 0 
		self.sent_count = 0
		self.word_count = 0

		""" now fill with input """
		textproc = TextProcessor(text,'nl_NL',"kpc")
		self.page_id = metadata['page_id']
		self.urltitle = metadata['page_title']
		self.lastrev_id = metadata['page_latest']
		self.version = metadata['page_touched'][6:8] + metadata['page_touched'][4:6] + metadata['page_touched'][0:4]

		if textproc.calculator.scores['sent_count'] >= 10 and textproc.calculator.scores['sentlen_average'] < 100:
			lines = textproc.text.split('\n')
			""" Not taking first line because before each line, a linebreak is inserted. """
			self.title = lines[1]
			self.description = self.re_htmltags.sub("",lines[3])
			
			self.updated = common.makeTimestamp(metadata['page_touched'])
			self.keywords = textproc.getKeywords()
			self.min_age = int(textproc.calculator.min_age)
			self.sent_count = textproc.calculator.scores['sent_count']
			self.word_count = textproc.calculator.scores['word_count']

			#self.printLOM()
			self.storeResults()
			#import sys
			#sys.exit()


	def storeResults(self):
		timestamp = int(time())
		c = self.DB.cursor()

		""" retrieve by page_id, if exists, update, else insert """
		query = "SELECT * FROM " + self.config["setspec"] + " WHERE page_id = " + str(self.page_id)
		c.execute(query)
		row = c.fetchone()
		
		if row:
			identifier = row['identifier']
			query = "UPDATE " + self.config["setspec"] + " SET title=%s, description=%s, lastrev_id=%s, version=%s, updated=%s, min_age=%s, sentences=%s, words=%s WHERE page_id = %s"
			c.execute(query, (self.title,self.description,self.lastrev_id,self.version,self.updated,self.min_age,self.sent_count,self.word_count,self.page_id))
			c.execute("""UPDATE oairecords SET updated=%s,deleted=0 WHERE identifier=%s""", (timestamp,identifier))
		else:
			identifier = uuid4()
			query = "INSERT INTO " + self.config["setspec"] + " (identifier, page_id, title, url_title, description, lastrev_id, version, updated, min_age, sentences, words) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
			c.execute(query, (identifier,self.page_id,self.title,self.urltitle,self.description,self.lastrev_id,self.version,self.updated,self.min_age,self.sent_count,self.word_count) )
			c.execute("""INSERT INTO oairecords (identifier,setspec,updated) VALUES ( %s, %s, %s )""", (identifier,self.config["setspec"],timestamp))

		c.close()


	def cleanup(self):
		file_prefix = self.config["work_dir"] + "/" + self.config["wiki"] + "-"
		extractdir = self.config["work_dir"] + "/extract-" + self.config["wiki"]

		self.logger.info("Cleaning up workdir files")
		self.FS.removeFile(file_prefix + "categorylinks.sql")
		self.FS.removeFile(file_prefix + "page.sql")
		self.FS.removeFile(file_prefix + "pages-articles.xml")

		for subdir in listdir(extractdir):
			self.FS.removeDir(extractdir + "/" + subdir)


	""" Debug """
	def printLOM(self):
		print "wiki: " + self.config["wiki"]
		print "general.title: " + self.title
		print "general.description: " + self.description
		print "general.keywords: " + ", ".join(self.keywords)
		print "lifecycle.version: " + self.version
		print "technical.location: " + "http://mediawikihost/index.php?title=" + self.urltitle + "&oldid=" + str(int(self.lastrev_id))
		print "educational.typicalagerange: " + str(int(self.fk.min_age)) + "+"
		print "classification.words: " + str( self.fk.scores['word_count'] )
		print "classification.sentences: " + str( self.fk.scores['sent_count'] ) 
