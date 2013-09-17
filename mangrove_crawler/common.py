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

	dict1 = {}
	options = Config.options(section)
	for option in options:
		try:
			dict1[option] = Config.get(section, option)
			if dict1[option] == -1:
				DebugPrint("skip: %s" % option)
		except:
			print("exception on %s!" % option)
			dict1[option] = None
	return dict1


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
