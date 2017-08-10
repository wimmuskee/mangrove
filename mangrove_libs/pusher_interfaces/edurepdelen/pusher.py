# -*- coding: utf-8 -*-

from mangrove_libs.interface import Interface
import requests

class Pusher(Interface):
	def __init__(self,config):
		Interface.__init__(self, config)
		self.endpoint = self.config["pusher_endpoint"]
		self.endpoint_key = self.config["pusher_key"]


	def pushAll(self):
		recordIds = []

		if self.DB.collection_updated > self.DB.collection_pushed:
			recordIDs = self.DB.getNewRecords()

		for record_id in recordIds:
			record = self.DB.getRecordById(record_id)
			
			if record["deleted"] == 1:
				self.delete(record["identifier"])
			else:
				self.push(record["identifier"],record["lom"])

		self.DB.touchCollection(self.startts,mode="push")


	def push(self,identifier,xml):
		try:
			r = requests.put(self.endpoint + self.endpoint_key + "/learning_object/" + identifier + "/lom", data={ "publish": "true", "data": xml })
			self.readGenericResponse(r.json())
		except Exception as err:
			self.logger.error("pushing failed: " + str(err))
			exit()


	# TODO, this does not work without xml
	def delete(self,identifier):
		try:
			r = requests.delete(self.endpoint + self.endpoint_key + "/learning_object/" + identifier)
			self.readGenericResponse(r.json())
		except Exception as err:
			self.logger.error("pushing delete failed: " + str(err))
			exit()


	def readGenericResponse(self,response):
		if response["status"] == "failed":
			raise RuntimeError(response["error"])
