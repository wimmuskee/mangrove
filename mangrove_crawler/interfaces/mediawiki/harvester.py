# -*- coding: utf-8 -*-

import common
from mangrove_crawler.textprocessing import getStopwords
from mangrove_crawler.common import downloadFile, removeFile, gzUnpack, bz2Unpack, checkLocal
import MySQLdb
import re
from bz2 import BZ2File
from subprocess import Popen, PIPE, call
from os import walk, path
from readability_score.calculators import fleschkincaid
from collections import defaultdict
from nltk import tokenize
from time import time
from uuid import uuid4

class Harvester:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
		self.config["dest_prefix"] = config["work_dir"] + "/" + config["wiki"] + "-"
		self.re_docid = re.compile(r'id="([0-9]*?)"')


	def harvest(self,part=""):
		""" Getting the data in 4 steps, parsing is distributed in parseExtracts """
		self.getData()
		#self.importData()
		#self.preprocessText()
		#self.parseExtracts()
		# delete extract dir


	def getData(self):
		src_prefix = self.config["download_path"] + self.config["wiki"] + "-"

		print "Downloading page sql file"
		downloadFile(src_prefix + "latest-page.sql.gz", self.config["dest_prefix"] + "page.sql.gz")
		print "Unpacking page sql file"
		gzUnpack(self.config["dest_prefix"] + "page.sql.gz",  self.config["dest_prefix"] + "page.sql" )

		print "Downloading page xml file"
		downloadFile(src_prefix + "latest-pages-articles.xml.bz2", self.config["dest_prefix"] + "pages-articles.xml.bz2")
		print "Unpacking page xml file"
		""" unpacking at shell level, wikipedia file too large for bz2 module """
		bzfile = self.config["dest_prefix"] + "pages-articles.xml.bz2"
		if path.isfile(bzfile):
			call("bunzip2 " + bzfile, shell=True)

		print "Downloading categories sql file"
		downloadFile(src_prefix + "latest-categorylinks.sql.gz", self.config["dest_prefix"] + "categorylinks.sql.gz")
		print "Unpacking categories sql file"
		gzUnpack(self.config["dest_prefix"] + "categorylinks.sql.gz", self.config["dest_prefix"] + "categorylinks.sql")

		print "Removing downloaded files"
		removeFile(self.config["dest_prefix"] + "page.sql.gz")
		removeFile(self.config["dest_prefix"] + "pages-articles.xml.bz2")
		removeFile(self.config["dest_prefix"] + "categorylinks.sql.gz")


	def importData(self):
		print "Importing data in database"
		sqlfiles = [self.config["dest_prefix"] + "page.sql", self.config["dest_prefix"] + "categorylinks.sql"]
		
		if checkLocal:
			db_prefix = "share/interfaces/mediawiki/"
		else:
			db_prefix = "/usr/share/mangrove/interfaces/mediawiki/"

		sqlfiles.extend([db_prefix + "importCategories.sql", db_prefix + "importCategoryRelations.sql"])
		sqlfiles.extend([db_prefix + self.config["wiki"] + "_removeSmallPages.sql", db_prefix + self.config["wiki"] + "_selectTitles.sql", db_prefix + self.config["wiki"] + "_renameTables.sql"])

		for sql in sqlfiles:
			process = Popen('mysql %s -u%s -p%s' % (self.config["db_name"], self.config["db_user"], self.config["db_passwd"]), stdout=PIPE, stdin=PIPE, shell=True)
			output = process.communicate(file(sql).read())


	def preprocessText(self):
		print "Preprocessing text"
		process = Popen('WikiExtractor.py -c -o %s' % (self.config["work_dir"] + "/extract-" + self.config["wiki"]), stdout=PIPE, stdin=PIPE, shell=True)
		output = process.communicate(file(self.config["dest_prefix"] + "pages-articles.xml").read())


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
				query = "SELECT * FROM " + self.config["wiki"] + "_page WHERE page_id = %s"
				c.execute(query, (id))
				row = c.fetchone()
				if row:
					self.setData(row,article)
			else:
				article += "\n" + line


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
		self.page_id = metadata[0]
		self.urltitle = metadata[2]
		self.lastrev_id = metadata[9]
		self.version = metadata[8][6:8] + metadata[8][4:6] + metadata[8][0:4]

		""" educational metadata """
		#self.fk = fleschkincaid.FleschKincaid(self.text.encode('utf-8'),'nl_NL')
		self.fk = fleschkincaid.FleschKincaid(self.text,'nl_NL')

		if self.fk.scores['sent_count'] >= 10 and self.fk.scores['sentlen_average'] < 100:
			self.updated = common.makeTimestamp(metadata[8])
			self.setTitleDescription()
			self.keywordExtract()

			self.printLOM()
			self.storeResults()
			#import sys
			#sys.exit()


	def setTitleDescription(self):
		lines = self.text.split('\n')
		""" Not taking first line because before each line, a linebreak is inserted. """
		self.title = lines[1]
		self.description = lines[3]


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
		query = "SELECT * FROM " + self.config["wiki"] + " WHERE page_id = %s"
		c.execute(query, (self.page_id))
		row = c.fetchone()
		
		if row:
			identifier = row[0]
			query = "UPDATE " + self.config["wiki"] + " SET title=%s, description=%s, lastrev_id=%s, version=%s, updated=%s, min_age=%s, sentences=%s, words=%s WHERE page_id = %s"
			c.execute(query, (self.title,self.description,self.lastrev_id,self.version,self.updated,int(self.fk.min_age),self.fk.scores['sent_count'],self.fk.scores['word_count'],self.page_id))
			c.execute("""UPDATE oairecords SET updated=%s WHERE identifier=%s""", (timestamp,identifier))
		else:
			identifier = uuid4()
			query = "INSERT INTO " + self.config["wiki"] + " (identifier, page_id, title, url_title, description, lastrev_id, version, updated, min_age, sentences, words) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
			c.execute(query, (identifier,self.page_id,self.title,self.urltitle,self.description,self.lastrev_id,self.version,self.updated,int(self.fk.min_age),self.fk.scores['sent_count'],self.fk.scores['word_count']) )
			c.execute("""INSERT INTO oairecords (identifier,setspec,updated) VALUES ( %s, %s, %s )""", (identifier,self.config["wiki"],timestamp))


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
