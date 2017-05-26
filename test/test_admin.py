from unittest import TestCase
from mangrove_libs import common
from mangrove_libs.admin import Admin
from storage.mysql import Database


class AdminfunctionsTestCase(TestCase):
	@classmethod
	def setUpClass(self):
		self.config = common.getConfig("mangrove-crawler.cfg.test", "common")
		self.DB = Database(self.config["db_host"],self.config["db_user"],self.config["db_passwd"],self.config["db_name"])

	def setUp(self):
		self.DB.initDB()

	def tearDown(self):
		self.DB.cleanupDB()


	def test_add_collection(self):
		admin = Admin(self.config)
		admin.addCollection("test_collection")
		self.assertTrue(self.DB.checkCollectionName("test_collection"))

	def test_add_duplicate_collection(self):
		admin = Admin(self.config)
		with self.assertRaises(RuntimeError):
			admin.addCollection("test_collection")
			admin.addCollection("test_collection")
