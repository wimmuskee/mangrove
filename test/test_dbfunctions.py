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

	def test_update_record(self):
		self.DB.updateRecord("","","orig:3")
		record = self.DB.getRecordById(1)
		self.assertGreater(record["updated"],1495857873)

	def test_insert_record(self):
		self.DB.setCollectionInfo("default_collection")
		self.DB.insertRecord("","","test","orig:4")
		record = self.DB.getRecordById(2)
		self.assertEqual(record["original_id"],"orig:4")
