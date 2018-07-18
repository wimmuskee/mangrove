from unittest import TestCase
from mangrove_libs import common
from mangrove_libs.admin import Admin
from storage.mysql import Database


class AdminfunctionsTestCase(TestCase):
	@classmethod
	def setUpClass(self):
		self.config = common.getConfig("mangrove-crawler-config.json.test", "common")
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
		admin.addCollection("test_collection")
		with self.assertRaises(RuntimeError):
			admin.addCollection("test_collection")

	def test_wrong_recordid_format(self):
		admin = Admin(self.config)
		with self.assertRaises(RuntimeError):
			admin.getRecordByInput("wrong-id-format")

	def test_integer_recordid_format(self):
		admin = Admin(self.config)
		record = admin.getRecordByInput("1")
		self.assertEqual(record["original_id"],"orig:3")

	def test_uuid_recordid_format(self):
		admin = Admin(self.config)
		record = admin.getRecordByInput("5d772545-041b-492e-9009-288445f8a453")
		self.assertEqual(record["original_id"],"orig:3")

	def test_delete_record(self):
		admin = Admin(self.config)
		admin.deleteRecord("5d772545-041b-492e-9009-288445f8a453")
		record = admin.getRecordByInput("5d772545-041b-492e-9009-288445f8a453")
		self.assertEqual(int(record["deleted"]),1)
