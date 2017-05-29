from unittest import TestCase
from mangrove_libs import common
from storage.filesystem import Filesystem


class FsfunctionsTestCase(TestCase):
	@classmethod
	def setUpClass(self):
		config = common.getConfig("mangrove-crawler.cfg.test", "common")
		self.FS = Filesystem(config)

	@classmethod
	def tearDownClass(self):
		self.FS.removeDir(self.FS.recordbase)


	def test_storerecord_invalid_input(self):
		with self.assertRaises(ValueError):
			self.FS.storeRecord("","xml","5d772545-041b-492e-9009-288445f8a453","testing")

		with self.assertRaises(ValueError):
			self.FS.storeRecord("test_collection","","5d772545-041b-492e-9009-288445f8a453","testing")

		with self.assertRaises(ValueError):
			self.FS.storeRecord("test_collection","xml","5d","testing")

	def test_storerecord_valid_input(self):
		self.FS.storeRecord("test_collection","xml","5d772545-041b-492e-9009-288445f8a453","testing")
		with open(self.FS.recordbase + "/test_collection/xml/5d/5d772545-041b-492e-9009-288445f8a453", "r") as f:
			data = f.read()

		self.assertEqual(data,"testing")


	def test_updaterecord(self):
		self.FS.storeRecord("test_collection","xml","5d772545-041b-492e-9009-288445f8a453","testing")
		self.FS.storeRecord("test_collection","xml","5d772545-041b-492e-9009-288445f8a453","testing_update")
		with open(self.FS.recordbase + "/test_collection/xml/5d/5d772545-041b-492e-9009-288445f8a453", "r") as f:
			data = f.read()

		self.assertEqual(data,"testing_update")
