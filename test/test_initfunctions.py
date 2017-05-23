from unittest import TestCase
from mangrove_crawler import common

class InitfunctionsTestCase(TestCase):
	def test_missing_dependency(self):
		with self.assertRaises(RuntimeError):
			common.checkPrograms(["this-dep-does-not-exist"])

	def test_available_dependency(self):
		common.checkPrograms(["python"])

	def test_common_config(self):
		config = common.getConfig("mangrove-crawler.cfg.test", "common")
		self.assertEqual(config["proxy_port"], "3128")
