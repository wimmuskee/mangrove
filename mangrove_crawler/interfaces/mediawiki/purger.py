# -*- coding: utf-8 -*-

from mangrove_crawler.common import getLogger
import MySQLdb
import MySQLdb.cursors
from time import time

class Purger:
	def __init__(self,config):
		self.config = config
		self.DB = MySQLdb.connect(host=config["db_host"],user=config["db_user"], passwd=config["db_passwd"],db=config["db_name"],use_unicode=1,cursorclass=MySQLdb.cursors.DictCursor)
		self.DB.set_character_set('utf8')
		self.logger = getLogger('mediawiki purger')


	def purge(self,method="",part=None):
		if method == "sync":
			self.sync()
		if method == "channel":
			self.logger.info("not working atm")
		if method == "file":
			self.logger.info("not working atm")
		if method == "record_id":
			self.logger.info("not working atm")


	def sync(self):
		self.logger.info("Purging all records that do not exist anymore in the downloaded " + self.config["wiki"] + " set.")
		c = self.DB.cursor()
		timestamp = int(time())
		current_oai = self.config["setspec"]
		current_source = self.config["setspec"] + "_page_current"
		
		""" Test first if the base database is present and valid. """
		query = "SHOW TABLES LIKE %s"
		c.execute(query, (current_source,))
		
		if c.rowcount != 1:
			self.logger.info("Base database " + current_source + " does not exist, harvest first.")
			quit()

		query = "SELECT COUNT(*) AS total FROM " + current_source
		c.execute(query)
		row = c.fetchone()

		if row["total"] == 0:
			self.logger.info("Base database " + current_source + " is empty, harvest first.")
			quit()

		""" If here, make diff, and delete """
		query = "SELECT oai.identifier FROM " + current_oai + " AS oai LEFT JOIN " + current_source + " AS src ON oai.page_id = src.page_id LEFT JOIN oairecords ON oai.identifier = oairecords.identifier WHERE src.page_id IS NULL AND oairecords.deleted = 0"
		c.execute(query)
		for row in c.fetchall():
			identifier = row["identifier"]
			c.execute("""UPDATE oairecords SET updated=%s, deleted=1 WHERE identifier=%s""", (timestamp,identifier))


		""" Finally, drop the table, the next update generates it again."""
		query = "DROP TABLE " + current_source
		c.execute(query)
		
		c.close()
