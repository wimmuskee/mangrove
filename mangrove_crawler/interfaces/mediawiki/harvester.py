# -*- coding: utf-8 -*-

import common
from mangrove_crawler.textprocessing import getStopwords
from mangrove_crawler.common import downloadFile, removeFile, removeDir, gzUnpack, bz2Unpack, checkLocal, getUrllib2Proxy
import MySQLdb
import MySQLdb.cursors
import re
from bz2 import BZ2File
from subprocess import Popen, PIPE, call
from os import walk, path, listdir
from readability_score.calculators import fleschkincaid
from collections import defaultdict
from nltk import tokenize
from time import time
from uuid import uuid4

class Harvester:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1,cursorclass=MySQLdb.cursors.DictCursor)
		self.DB.set_character_set('utf8')
		self.httpProxy = None
		self.config["dest_prefix"] = config["work_dir"] + "/" + config["wiki"] + "-"
		self.re_docid = re.compile(r'id="([0-9]*?)"')
		self.re_htmltags = re.compile('<[^<]+?>')

		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getUrllib2Proxy(self.config["proxy_host"],self.config["proxy_port"])

		if checkLocal:
			self.share_prefix = "share/interfaces/mediawiki/"
		else:
			self.share_prefix = "/usr/share/mangrove/interfaces/mediawiki/"


	def harvest(self,part=""):
		""" Getting the data in 4 steps, parsing is distributed in parseExtracts """
		self.getData()
		self.importData()
		self.preprocessText()
		self.parseExtracts()
		self.cleanup()


	def getData(self):
		src_prefix = self.config["download_path"] + self.config["wiki"] + "-"

		print "Downloading page sql file"
		downloadFile(self.httpProxy, src_prefix + "latest-page.sql.gz", self.config["dest_prefix"] + "page.sql.gz")
		print "Unpacking page sql file"
		gzUnpack(self.config["dest_prefix"] + "page.sql.gz",  self.config["dest_prefix"] + "page.sql" )

		print "Downloading page xml file"
		downloadFile(self.httpProxy, src_prefix + "latest-pages-articles.xml.bz2", self.config["dest_prefix"] + "pages-articles.xml.bz2")
		print "Unpacking page xml file"
		""" unpacking at shell level, wikipedia file too large for bz2 module """
		bzfile = self.config["dest_prefix"] + "pages-articles.xml.bz2"
		if path.isfile(bzfile):
			call("bunzip2 " + bzfile, shell=True)

		print "Downloading categories sql file"
		downloadFile(self.httpProxy, src_prefix + "latest-categorylinks.sql.gz", self.config["dest_prefix"] + "categorylinks.sql.gz")
		print "Unpacking categories sql file"
		gzUnpack(self.config["dest_prefix"] + "categorylinks.sql.gz", self.config["dest_prefix"] + "categorylinks.sql")

		print "Removing downloaded files"
		removeFile(self.config["dest_prefix"] + "page.sql.gz")
		removeFile(self.config["dest_prefix"] + "pages-articles.xml.bz2")
		removeFile(self.config["dest_prefix"] + "categorylinks.sql.gz")


	def importData(self):
		print "Importing data in database"
		sqlfiles = [self.config["dest_prefix"] + "page.sql", self.config["dest_prefix"] + "categorylinks.sql"]
		
		sqlfiles.extend([self.share_prefix + "importCategories.sql", self.share_prefix + "importCategoryRelations.sql"])
		sqlfiles.extend([self.share_prefix + self.config["wiki"] + "_removeSmallPages.sql", self.share_prefix + self.config["wiki"] + "_selectTitles.sql", self.share_prefix + self.config["wiki"] + "_renameTables.sql"])

		for sql in sqlfiles:
			process = Popen('mysql %s -u%s -p%s' % (self.config["db_name"], self.config["db_user"], self.config["db_passwd"]), stdout=PIPE, stdin=PIPE, shell=True)
			output = process.communicate(file(sql).read())


	def preprocessText(self):
		print "Preprocessing text"
		outputdir = self.config["work_dir"] + "/extract-" + self.config["wiki"]
		inputfile = self.config["dest_prefix"] + "pages-articles.xml"
		script = self.share_prefix + "WikiExtractorWrapper.sh"
		call([script, inputfile, outputdir])


	def parseExtracts(self):
		print "Parse text extracts"
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
		self.text = ""

		""" now fill with input """
		self.text = text
		self.page_id = metadata['page_id']
		self.urltitle = metadata['page_title']
		self.lastrev_id = metadata['page_latest']
		self.version = metadata['page_touched'][6:8] + metadata['page_touched'][4:6] + metadata['page_touched'][0:4]

		""" educational metadata """
		#self.fk = fleschkincaid.FleschKincaid(self.text.encode('utf-8'),'nl_NL')
		self.fk = fleschkincaid.FleschKincaid(self.text,'nl_NL')

		if self.fk.scores['sent_count'] >= 10 and self.fk.scores['sentlen_average'] < 100:
			self.updated = common.makeTimestamp(metadata['page_touched'])
			self.setTitleDescription()
			self.keywordExtract()

			#self.printLOM()
			self.storeResults()
			#import sys
			#sys.exit()


	def setTitleDescription(self):
		lines = self.text.split('\n')
		""" Not taking first line because before each line, a linebreak is inserted. """
		self.title = lines[1]
		self.description = self.re_htmltags.sub("",lines[3])


	def keywordExtract(self):
		""" not using work_tokenize because of unicode characters """
		ignored_words = getStopwords()
		words = tokenize.wordpunct_tokenize( self.text )
		filtered_words = [w.lower() for w in words if not w.lower() in ignored_words]
		include_threshold = round(self.fk.scores['word_count'] * 0.003)

		""" collect word statistics """
		counts = defaultdict(int) 
		for word in filtered_words:
			if word.isalpha() and len(word) > 1:
				counts[word] += 1

		""" only most occuring; filter words based on threshhold """
		keys = defaultdict(int)
		for word,count in counts.items():
			if count > include_threshold:
				keys[word] = count

		keywords = sorted(keys, key=keys.get, reverse=True)
		self.keywords = keywords[:15]


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
			c.execute(query, (self.title,self.description,self.lastrev_id,self.version,self.updated,int(self.fk.min_age),self.fk.scores['sent_count'],self.fk.scores['word_count'],self.page_id))
			c.execute("""UPDATE oairecords SET updated=%s WHERE identifier=%s""", (timestamp,identifier))
		else:
			identifier = uuid4()
			query = "INSERT INTO " + self.config["setspec"] + " (identifier, page_id, title, url_title, description, lastrev_id, version, updated, min_age, sentences, words) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
			c.execute(query, (identifier,self.page_id,self.title,self.urltitle,self.description,self.lastrev_id,self.version,self.updated,int(self.fk.min_age),self.fk.scores['sent_count'],self.fk.scores['word_count']) )
			c.execute("""INSERT INTO oairecords (identifier,setspec,updated) VALUES ( %s, %s, %s )""", (identifier,self.config["setspec"],timestamp))

		c.close()


	def cleanup(self):
		file_prefix = self.config["work_dir"] + "/" + self.config["wiki"] + "-"
		extractdir = self.config["work_dir"] + "/extract-" + self.config["wiki"]

		print("Cleaning up workdir files")
		removeFile(file_prefix + "categorylinks.sql")
		removeFile(file_prefix + "page.sql")
		removeFile(file_prefix + "pages-articles.xml")

		for subdir in listdir(extractdir):
			removeDir(extractdir + "/" + subdir)


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
