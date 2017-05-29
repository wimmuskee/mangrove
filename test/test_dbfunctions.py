from unittest import TestCase
from mangrove_libs import common
from storage.mysql import Database
from time import time


class DbfunctionsTestCase(TestCase):
	@classmethod
	def setUpClass(self):
		config = common.getConfig("mangrove-crawler.cfg.test", "common")
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"])


	def setUp(self):
		self.DB.initDB()

	def tearDown(self):
		self.DB.cleanupDB()


	def test_touch_collection_updated(self):
		self.DB.setCollectionInfo("default_collection")
		self.DB.touchCollection()
		self.assertGreaterEqual(self.DB.collection_updated,int(time()))

	def test_touch_collection_pushed(self):
		self.DB.setCollectionInfo("default_collection")
		self.DB.touchCollection(mode="push")
		self.assertGreaterEqual(self.DB.collection_pushed,int(time()))
