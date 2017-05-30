# -*- coding: utf-8 -*-
import os
from shutil import rmtree

class Filesystem:
	def __init__(self,config):
		self.recordbase = config["fs_recordbase"].rstrip("/")
		self.workdir = config["fs_workbase"].rstrip("/") + "/" + config["configuration"]
		self.makeDir(self.recordbase)
		self.makeDir(self.workdir)


	def storeRecord(self,collection,format,identifier,data):
		if not collection or not format:
			raise ValueError("collection or format not specified")

		if len(identifier) <= 2:
			raise ValueError("identifier should have a length of at least 3")

		targetdir = self.recordbase + "/" + collection + "/" + format + "/" + identifier[0:2]
		self.makeDir(targetdir)

		with open(targetdir + "/" + identifier, "w") as f:
			f.write(data)


	def removeDir(self,dir):
		try:
			rmtree(dir)
		except:
			pass


	def removeFile(filename):
		try:
			os.remove(filename)
		except OSError:
			pass


	def makeDir(self,dir):
		if not os.path.exists(dir):
			os.makedirs(dir)
