# -*- coding: utf-8 -*-

from mangrove_crawler.common import downloadFile, removeFile, gzUnpack, bz2Unpack, checkLocal
import MySQLdb
from subprocess import Popen, PIPE

class HarvestDatabase:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1)
		self.config["dest_prefix"] = config["work_dir"] + "/" + config["wiki"] + "-"


	def harvest(self):
		self.getData()
		self.importData()
		self.preprocessText()


	def getData(self):
		src_prefix = self.config["download_path"] + self.config["wiki"] + "-"

		print "Downloading page sql file"
		downloadFile(src_prefix + "latest-page.sql.gz", self.config["dest_prefix"] + "page.sql.gz")
		print "Unpacking page sql file"
		gzUnpack(self.config["dest_prefix"] + "page.sql.gz",  self.config["dest_prefix"] + "page.sql" )

		print "Downloading page xml file"
		downloadFile(src_prefix + "latest-pages-articles.xml.bz2", self.config["dest_prefix"] + "pages-articles.xml.bz2")
		print "Unpacking page xml file"
		bz2Unpack(self.config["dest_prefix"] + "pages-articles.xml.bz2",  self.config["dest_prefix"] + "pages-articles.xml" )

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
		process = Popen('WikiExtractor -c -o %s' % (self.config["work_dir"] + "/extract-" + self.config["wiki"]), stdout=PIPE, stdin=PIPE, shell=True)
		output = process.communicate(file(self.config["dest_prefix"] + "pages-articles.xml").read())


# delete extract dir