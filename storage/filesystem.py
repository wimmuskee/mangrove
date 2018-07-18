# -*- coding: utf-8 -*-
import os
from shutil import rmtree

class Filesystem:
	def __init__(self,config):
		self.recorddir = config["fs_recordbase"].rstrip("/") + "/" + config["configuration"]
		self.workdir = config["fs_workbase"].rstrip("/") + "/" + config["configuration"]
		self.makeDir(self.recorddir)
		self.makeDir(self.workdir)


	def storeRecord(self,format,identifier,data):
		if not format:
			raise ValueError("format not specified")

		if len(identifier) <= 2:
			raise ValueError("identifier should have a length of at least 3")

		targetdir = self.recorddir + "/" + format + "/" + identifier[0:2]
		self.makeDir(targetdir)

		with open(targetdir + "/" + identifier, "w") as f:
			f.write(data)

	def removeDir(self,dir):
		try:
			rmtree(dir)
		except:
			pass

	def removeFile(self,filename):
		try:
			os.remove(filename)
		except OSError:
			pass

	def makeDir(self,dir):
		if not os.path.exists(dir):
			os.makedirs(dir)

	def cleanupFS(self):
		""" Remove record and workdir after testing. """
		self.removeDir(self.recorddir)
		self.removeDir(self.workdir)
