# -*- coding: utf-8 -*-
"""
This module contains common functions
for the mangrove crawler.

Wim Muskee, 2013
wimmuskee@gmail.com

License: GPL-3
"""

def getConfig(configfile,section):
	import ConfigParser
	Config = ConfigParser.ConfigParser()
	Config.read(configfile)

	config_options = {}
	options = Config.options(section)
	for option in options:
		try:
			config_options[option] = Config.get(section, option)
			if config_options[option] == -1:
				DebugPrint("skip: %s" % option)
		except:
			print("exception on %s!" % option)
			config_options[option] = None

	return config_options


""" Dynamically import a method """
def import_from(module, name):
	import importlib
	module = __import__(module, fromlist=[name])
	return getattr(module, name)


def downloadFile(source,dest):
	import urllib2
	f = urllib2.urlopen(source)
	output = open(dest,'wb')
	output.write(f.read())
	output.close()


def removeFile(filename):
	from os import remove
	try:
		remove(filename)
	except OSError:
		pass

def removeDir(dirname):
	from shutil import rmtree
	try:
		rmtree(dirname)
	except:
		pass

def gzUnpack(source,dest):
	import gzip
	f = gzip.open(source,'rb')
	output = open(dest,'wb')
	output.write(f.read())
	output.close()


def bz2Unpack(source,dest):
	from bz2 import BZ2File
	f = BZ2File( source, 'r')
	output = open(dest,'wb')
	output.write(f.read())
	output.close()


def checkLocal():
	from os import path, getcwd
	if path.isdir( getcwd() + "/share" ):
		return true
	else:
		return false
