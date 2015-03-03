# -*- coding: utf-8 -*-
"""
This module contains common functions
for the mangrove crawler.

Wim Muskee, 2013-2015
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
			if option == "proxy_use":
				config_options["proxy_use"] = Config.getboolean(section, "proxy_use")
			else:
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


""" Download a file using chunks to deal with large files. """
def downloadFile(httpProxy,source,dest):
	import requests
	r = requests.get(source, stream=True, proxies=httpProxy)
	with open(dest, 'wb') as f:
		for chunk in r.iter_content(chunk_size=1024):
			if chunk: # filter out keep-alive new chunks
				f.write(chunk)
				f.flush()


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


def getHttplib2Proxy(proxy_host,proxy_port):
	import httplib2
	import socks
	return httplib2.Http(proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP, proxy_host, int(proxy_port), False))


# not actively used, keeping it however, just in case ...
def getUrllib2Proxy(proxy_host,proxy_port):
	import urllib2
	return urllib2.ProxyHandler({"http": proxy_host + ":" + proxy_port})


def getRequestsProxy(proxy_host,proxy_port):
	return { "http": proxy_host + ":" + proxy_port }
