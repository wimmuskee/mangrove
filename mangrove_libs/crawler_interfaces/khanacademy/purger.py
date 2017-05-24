# -*- coding: utf-8 -*-

from mangrove_crawler.interfaces.youtube.common import getVideoAvailableStatus
from mangrove_crawler.interface import Interface
from time import sleep


class Purger(Interface):
	"""khan academy purger"""

	def __init__(self,config):
		Interface.__init__(self, config)
		Interface.handleHttplib2Proxy(self)


	# keeping method and part for now, for backwards compatibility with youtube
	def purge(self,method="",part=None):
		self.logger.info("Purging all khanacademy video's that are not available.")

		for row in self.DB.getUndeleted():
			if not getVideoAvailableStatus(self.httpProxy,self.config["developer_key"],row["original_id"]):
				self.logger.info("purging: " + row["identifier"])
				self.DB.deleteRecord(row["identifier"])
			
			sleep(1)
