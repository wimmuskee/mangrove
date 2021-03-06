from unittest import TestCase
from mangrove_libs import common
from mangrove_libs.interface import Interface
from storage.mysql import Database
import re

class InterfaceTestCase(TestCase):
	@classmethod
	def setUpClass(self):
		config = common.getConfig("mangrove-crawler-config.json.test", "default_collection")
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"])
		self.DB.initDB()
		self.interface = Interface(config)

	@classmethod
	def tearDownClass(self):
		self.interface.FS.cleanupFS()
		del self.interface
		self.DB.cleanupDB()


	def test_collection_id(self):
		self.assertEqual(self.interface.DB.collection_id, 1)

	def test_collection_updated(self):
		self.assertEqual(self.interface.DB.collection_updated, 0)

	def test_collection_pushed(self):
		self.assertEqual(self.interface.DB.collection_pushed, 0)

	def test_startts(self):
		self.assertGreater(self.interface.startts,1490000000)

	def test_requests_proxy(self):
		self.interface.handleRequestsProxy()
		self.assertEqual(self.interface.httpProxy["http"], "localhost:3128")

	def test_new_identifier_uuid(self):
		identifier = self.interface.getNewIdentifier()
		self.assertTrue(re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$', identifier))
