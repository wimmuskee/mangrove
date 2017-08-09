# -*- coding: utf-8 -*-

from mangrove_libs.interface import Interface

class Purger(Interface):
	"""mediawiki purger"""

	def __init__(self,config):
		Interface.__init__(self, config)


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
		c = self.DB.DB.cursor()
		current_source = self.config["setspec"] + "_page"

		""" Test first if the base database is present and valid. """
		query = "SHOW TABLES LIKE %s"
		c.execute(query, (current_source,))

		if c.rowcount != 1:
			self.logger.info("Base database " + current_source + " does not exist, harvest first.")
			exit()

		c.execute("SELECT COUNT(*) AS total FROM " + current_source)
		row = c.fetchone()

		if row["total"] == 0:
			self.logger.info("Base database " + current_source + " is empty, harvest first.")
			exit()()

		""" If here, make diff, and delete """
		query = "SELECT oai.identifier FROM oairecords AS oai LEFT JOIN " + current_source + " AS src ON oai.original_id = src.page_title WHERE src.page_title IS NULL and oai.collection_id = %s and oai.deleted = 0;"
		c.execute(query,(self.DB.collection_id,))
		for row in c.fetchall():
			self.DB.deleteRecord(row["identifier"])

		c.close()
