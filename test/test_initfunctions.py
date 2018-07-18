from unittest import TestCase
from mangrove_libs import common

class InitfunctionsTestCase(TestCase):
	def test_missing_dependency(self):
		with self.assertRaises(RuntimeError):
			common.checkPrograms(["this-dep-does-not-exist"])

	def test_available_dependency(self):
		common.checkPrograms(["python"])

	def test_common_config(self):
		config = common.getConfig("mangrove-crawler-config.json.test", "common")
		self.assertEqual(config["proxy_port"], "3128")

	def test_configuration_config(self):
		config = common.getConfig("mangrove-crawler-config.json.test", "default_collection")
		self.assertEqual(config["proxy_host"], "localhost")
