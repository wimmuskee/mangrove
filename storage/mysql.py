# -*- coding: utf-8 -*-

import MySQLdb
import MySQLdb.cursors
from time import time
from uuid import uuid4


class Database:
	def __init__(self,db_host,db_user,db_passwd,db_name,collection):
		self.DB = MySQLdb.connect(host=db_host,user=db_user, passwd=db_passwd,db=db_name,use_unicode=1,cursorclass=MySQLdb.cursors.DictCursor)
		self.DB.set_character_set('utf8')
		self.setCollectionInfo(collection)


	def setCollectionInfo(self,collection):
		c = self.DB.cursor()
		query = "SELECT id,updated FROM collections WHERE configuration = %s"
		c.execute(query,(collection,))
		row = c.fetchone()
		self.collection_id = row["id"]
		self.collection_updated = row["updated"]


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


	def touchCollection(self):
		c = self.DB.cursor()
		timestamp = int(time())
		query = "UPDATE collections SET updated=%s WHERE id=%s"
		c.execute(query,(timestamp,self.collection_id))
		self.DB.commit()
