# -*- coding: utf-8 -*-

from storage.mysql import Database
from storage.filesystem import Filesystem
from mangrove_libs.common import getLogger, getRequestsProxy, getHttplib2Proxy
from time import time
from uuid import uuid4


class Interface:
	def __init__(self,config):
		self.config = config
		self.DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"])
		self.FS = Filesystem(config)
		self.httpProxy=None
		self.logger = getLogger(self.__doc__)
		self.startts = int(time())
		self.DB.setCollectionInfo(config["configuration"])


	def handleRequestsProxy(self):
		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getRequestsProxy(self.config["proxy_host"],self.config["proxy_port"])


	def handleHttplib2Proxy(self):
		if self.config["proxy_host"] and self.config["proxy_use"]:
			self.httpProxy = getHttplib2Proxy(self.config["proxy_host"],self.config["proxy_port"])


	def getNewIdentifier(self):
		return str(uuid4())


	def storeResult(self,record,setspec,lom,oaidc):
		""" retrieve by page_id, if exists, update, else insert """
		row = self.DB.getRecordByOriginalId(record["original_id"])

		if row:
			self.DB.updateRecord(lom,oaidc,record["original_id"])
			identifier = record["identifier"]
		else:
			identifier = self.getNewIdentifier()
			self.DB.insertRecord(identifier,lom,oaidc,setspec,record["original_id"])

		# always update fs record
		self.FS.storeRecord("lom",identifier,lom)
		self.FS.storeRecord("oaidc",identifier,oaidc)
