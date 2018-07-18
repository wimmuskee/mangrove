from unittest import TestCase
from mangrove_libs import common
from mangrove_libs.crawler_interfaces.mediawiki import harvester
from storage.mysql import Database
from shutil import copyfile

class CrawlerMediawikiTestCase(TestCase):
	@classmethod
	def setUpClass(self):
		config = common.getConfig("mangrove-crawler.cfg.test", "common")
		config.update(common.getConfig("mangrove-crawler.cfg.test","mediawiki"))
		# init db first
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"])
		self.DB.initDB()
		self.Harvester = harvester.Harvester(config,testing=True)

	@classmethod
	def tearDownClass(self):
		self.Harvester.FS.cleanupFS()
		del self.Harvester
		self.DB.cleanupDB()


	def test_normal_processing(self):
		copyfile("test/files/mediawiki-single-article.xml", self.Harvester.FS.workdir + "/pages-articles.xml")
		self.Harvester.readXmlExport()
		self.assertEqual(self.Harvester.recordmeta["title"], "Mercurius (planeet)")
		self.assertEqual(self.Harvester.recordmeta["revision_id"], "432728")
		self.assertEqual(self.Harvester.recordmeta["timestamp"], "2016-02-14T14:08:47Z")
		self.assertIn("Mercurius",self.Harvester.recordtext)

	def test_wrong_namespace(self):
		copyfile("test/files/mediawiki-wrong-namespace.xml", self.Harvester.FS.workdir + "/pages-articles.xml")
		self.Harvester.readXmlExport()
		self.assertIsNone(self.Harvester.recordmeta["title"])

	def test_small_page(self):
		copyfile("test/files/mediawiki-small-page.xml", self.Harvester.FS.workdir + "/pages-articles.xml")
		self.Harvester.readXmlExport()
		self.assertIsNone(self.Harvester.recordmeta["title"])

	def test_missing_metafield(self):
		copyfile("test/files/mediawiki-missing-metafield.xml", self.Harvester.FS.workdir + "/pages-articles.xml")
		self.Harvester.readXmlExport()
		self.assertEqual(self.Harvester.recordtext, "")

	def test_empty_metafield(self):
		copyfile("test/files/mediawiki-empty-metafield.xml", self.Harvester.FS.workdir + "/pages-articles.xml")
		self.Harvester.readXmlExport()
		self.assertEqual(self.Harvester.recordtext, "")

	def test_existing_older(self):
		copyfile("test/files/mediawiki-older-than-db.xml", self.Harvester.FS.workdir + "/pages-articles.xml")
		self.Harvester.readXmlExport()
		self.assertEqual(self.Harvester.recordtext, "")

	def test_filter_listof(self):
		copyfile("test/files/mediawiki-list-of.xml", self.Harvester.FS.workdir + "/pages-articles.xml")
		self.Harvester.readXmlExport()
		self.assertEqual(self.Harvester.recordtext, "")

	def test_dataset(self):
		self.Harvester.recordmeta = {"title": "Mercurius (planeet)", "revision_id": "432728", "timestamp": "2016-02-14T14:08:47Z"}
		self.Harvester.recordtext = "Mercurius is de dichtstbijzijnde planeet bij de zon.\nMercurius is genoemd naar de boodschapper van de Romeinse goden."
		self.Harvester.setData()
		self.assertEqual(self.Harvester.record["title"], "Mercurius (planeet)")
		self.assertEqual(self.Harvester.record["description"], "Mercurius is de dichtstbijzijnde planeet bij de zon.")
		self.assertEqual(self.Harvester.record["identifier"][0]["value"], "http://examplewiki.com/Mercurius_(planeet)")
		self.assertEqual(self.Harvester.record["version"], "20160214")
		self.assertEqual(self.Harvester.record["publishdate"], "2016-02-14T14:08:47Z")
		self.assertEqual(self.Harvester.record["location"], "http://examplewiki.com/Mercurius_(planeet)")
		self.assertEqual(self.Harvester.record["isversionof"], "http://examplewiki.com/index.php?title=Mercurius_(planeet)&oldid=432728")
		self.assertIn("mercurius",self.Harvester.record["keywords"])
		self.assertIn("PO",self.Harvester.record["context"])
		self.assertIn("VO",self.Harvester.record["context"])
