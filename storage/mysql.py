# -*- coding: utf-8 -*-

import MySQLdb
import MySQLdb.cursors
from time import time
from uuid import uuid4


class Database:
	def __init__(self,db_host,db_user,db_passwd,db_name):
		self.DB = MySQLdb.connect(host=db_host,user=db_user, passwd=db_passwd,db=db_name,use_unicode=1,cursorclass=MySQLdb.cursors.DictCursor)
		self.DB.set_character_set('utf8')


	def setCollectionInfo(self,collection):
		c = self.DB.cursor()
		query = "SELECT id,updated,pushed FROM collections WHERE configuration = %s"
		c.execute(query,(collection,))
		row = c.fetchone()
		self.collection_id = int(row["id"])
		self.collection_updated = int(row["updated"])
		self.collection_pushed = int(row["pushed"])


	def getAll(self):
		c = self.DB.cursor()
		query = "SELECT * FROM oairecords WHERE collection_id = %s"
		c.execute(query, (self.collection_id,))
		return c.fetchall()


	def getUndeleted(self):
		c = self.DB.cursor()
		query = "SELECT identifier,original_id FROM oairecords WHERE collection_id = %s AND deleted = 0"
		c.execute(query, (self.collection_id,))
		return c.fetchall()


	def getUpdatedByOriginalId(self,original_id):
		c = self.DB.cursor()
		query = "SELECT updated FROM oairecords WHERE original_id = %s"
		c.execute(query, (original_id,))
		return c.fetchone()


	def getRecordByIdentifier(self,identifier):
		c = self.DB.cursor()
		query = "SELECT * FROM oairecords WHERE identifier = %s"
		c.execute(query, (identifier,))
		return c.fetchone()


	def getRecordById(self,id):
		c = self.DB.cursor()
		query = "SELECT * FROM oairecords WHERE counter = %s"
		c.execute(query, (id,))
		return c.fetchone()


	def updateRecord(self,lom,oaidc,original_id):
		c = self.DB.cursor()
		timestamp = int(time())
		query = "UPDATE oairecords SET updated=%s, lom=%s, oaidc=%s WHERE original_id=%s"
		c.execute(query,(timestamp,lom,oaidc,original_id))
		self.DB.commit()


	def insertRecord(self,lom,oaidc,setspec,original_id):
		c = self.DB.cursor()
		timestamp = int(time())
		identifier = uuid4()
		query = "INSERT INTO oairecords (identifier,original_id,collection_id,setspec,updated,lom,oaidc) VALUES ( %s, %s, %s, %s, %s, %s, %s )"
		c.execute(query, (identifier,original_id,self.collection_id,setspec,timestamp,lom,oaidc))
		self.DB.commit()


	def deleteRecord(self,identifier):
		c = self.DB.cursor()
		timestamp = int(time())
		query = "UPDATE oairecords SET updated=%s, deleted=1 WHERE identifier=%s"
		c.execute(query, (timestamp,identifier))
		self.DB.commit()


	def touchCollection(self,timestamp=None,mode="crawl"):
		c = self.DB.cursor()
		if not timestamp:
			timestamp = int(time())

		if mode == "crawl":
			query = "UPDATE collections SET updated=%s WHERE id=%s"
			self.collection_updated = timestamp
		elif mode == "push":
			query = "UPDATE collections SET pushed=%s WHERE id=%s"
			self.collection_pushed = timestamp
		else:
			raise LookupError("wrong mode: " + mode)

		c.execute(query,(timestamp,self.collection_id))
		self.DB.commit()


	def getCollections(self):
		c = self.DB.cursor()
		query = "SELECT * FROM collections"
		c.execute(query)
		return c.fetchall()


	def checkCollectionName(self,collection):
		c = self.DB.cursor()
		query = "SELECT * FROM collections WHERE configuration = %s"
		c.execute(query,(collection,))
		row = c.fetchone()
		
		if row:
			return True
		else:
			return False


	def addCollection(self,collection):
		c = self.DB.cursor()
		query = "INSERT INTO collections (configuration) VALUES ( %s )"
		c.execute(query,(collection,))
		self.DB.commit()


	def getCounts(self):
		c = self.DB.cursor()
		query = "SELECT configuration, deleted, count(*) AS count FROM oairecords AS oai LEFT JOIN collections AS c ON oai.collection_id = c.id GROUP BY collection_id, deleted"
		c.execute(query)
		return c.fetchall()


	def getNewRecords(self):
		c = self.DB.cursor()
		ids = []
		query = "SELECT counter FROM oairecords WHERE collection_id = %s AND updated > %s"
		c.execute(query,(self.collection_id,self.collection_pushed))
		for row in c.fetchall():
			ids.append(row["counter"])
		return ids


	def initDB(self):
		self.__executeFile("share/sql/collections.sql")
		self.__executeFile("share/sql/oairecords.sql")
		self.__executeFile("share/sql/test_records.sql")
		self.__executeFile("share/sql/test_collections.sql")


	def cleanupDB(self):
		c = self.DB.cursor()
		c.execute("DROP TABLE collections")
		c.execute("DROP TABLE oairecords")
		self.DB.commit()


	def __executeFile(self,sqlfile):
		with open(sqlfile, "r") as f:
			sql = f.read()

		sqlcommands = sql.split(';')
		c = self.DB.cursor()
		for line in sqlcommands:
			command = line.strip()
			if command:
				c.execute(command)

		self.DB.commit()
