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


	def getRecord(self,recordid):
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
		
		x = prettytable.PrettyTable(["id", "identifier", "original_id", "setspec", "collection_id", "deleted", "updated"])
		x.add_row([r["counter"], r["identifier"], r["original_id"], r["setspec"], r["collection_id"],r["deleted"], r["updated"]])
		print(x)
