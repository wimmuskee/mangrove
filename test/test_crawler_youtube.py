from unittest import TestCase
from mangrove_libs import common
from mangrove_libs.crawler_interfaces.youtube import harvester
from storage.mysql import Database
import json

class CrawlerYoutubeTestCase(TestCase):
	@classmethod
	def setUpClass(self):
		config = common.getConfig("mangrove-crawler-config.json.test", "default_collection")
		# init db first
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"])
		self.DB.initDB()
		self.Harvester = harvester.Harvester(config,testing=True)

	@classmethod
	def tearDownClass(self):
		self.Harvester.FS.cleanupFS()
		del self.Harvester
		self.DB.cleanupDB()


	def test_dataset(self):
		channel = { 
			"language": "nl",
			"title": "Scheikundelessen",
			"username": "JoeCurie",
			"keyword": ["scheikunde"],
			"context": ["VO"],
			"learningresourcetype": "informatiebron",
			"intendedenduserrole": "learner",
			"discipline": [ { "id": "3aab168a-9b24-4aca-b0f1-4bfb12e7c288", "value": "Scheikunde" } ] }

		with open("test/files/youtube-single-result.json", "r") as f:
			self.Harvester.page = json.loads(f.read())

		self.Harvester.setData(channel)
		self.assertEqual(self.Harvester.record["title"], "Reactiemechanismen")
		self.assertIn("scheikunde",self.Harvester.record["keyword"])
		self.assertEqual(self.Harvester.record["identifier"][1]["value"], "http://youtu.be/FHNNibywwe7")
		self.assertEqual(self.Harvester.record["contribute"][1]["role"], "author")
		self.assertEqual(self.Harvester.record["location"], "http://youtu.be/FHNNibywwe7")
		self.assertEqual(self.Harvester.record["duration"], "PT370S")
		self.assertEqual(self.Harvester.record["copyrightandotherrestrictions"], "yes")
		self.assertIn("http://www.youtube.com/t/terms", self.Harvester.record["copyrightdescription"])
		self.assertIn("VO",self.Harvester.record["educational"][0]["context"])
		self.assertEqual(self.Harvester.record["relation"][0]["resource"]["catalogentry"][0]["entry"], "https://i.ytimg.com/vi/FHNNibywwe7/default.jpg")
		self.assertEqual(self.Harvester.record["relation"][1]["resource"]["catalogentry"][0]["entry"], "http://www.youtube.com/embed/FHNNibywwe7")
		self.assertEqual(self.Harvester.record["classification"][0]["taxonpath"][0]["taxon"][0]["id"], "3aab168a-9b24-4aca-b0f1-4bfb12e7c288")
