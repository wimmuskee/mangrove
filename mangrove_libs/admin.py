# -*- coding: utf-8 -*-

from storage.mysql import Database
# perhaps make separate formatter for prettytable
import prettytable
import re

class Admin:
	def __init__(self,config):
		self.config = config
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"])


	def getCollections(self):
		collections = self.DB.getCollections()
		x = prettytable.PrettyTable(["id", "collection", "updated"])
		for row in collections:
			x.add_row([row["id"], row["configuration"], row["updated"]])
		print(x)


	def getStats(self):
		counts = self.DB.getCounts()
		x = prettytable.PrettyTable(["collection", "deleted", "count"])
		for row in counts:
			x.add_row([row["configuration"],row["deleted"],row["count"]])
		print(x)


	def addCollection(self,collection):
		if not self.DB.checkCollectionName(collection):
			self.DB.addCollection(collection)
		else:
			print("collection name exists: " + collection)
			exit()
		

	def getRecord(self,recordid,field):
		r = self.getRecordByInput(recordid)
		
		if field:
			print(r[field])
		else:
			x = prettytable.PrettyTable(["id", "identifier", "original_id", "setspec", "collection_id", "deleted", "updated"])
			x.add_row([r["counter"], r["identifier"], r["original_id"], r["setspec"], r["collection_id"],r["deleted"], r["updated"]])
			print(x)


	def deleteRecord(self,recordid):
		r = self.getRecordByInput(recordid)
		self.DB.deleteRecord(r["identifier"])


	def getRecordByInput(self,recordid):
		# first determine id or identifier 
		re_number = re.compile('[0-9]+')
		re_uuid = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
		
		if re_number.match(recordid):
			r = self.DB.getRecordById(recordid)
		elif re_uuid.match(recordid):
			r = self.DB.getRecordByIdentifier(recordid)
		else:
			print("cannot locate record, use id or identifier: " + recordid)
			exit()
			
		return r
