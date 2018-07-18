# -*- coding: utf-8 -*-
"""
This module contains common functions
for the mangrove crawler.

Wim Muskee, 2013-2018
wimmuskee@gmail.com

License: GPL-3
"""

def getConfig(configfile,section):
	import json
	with open(configfile, "r") as f:
		configdata = json.loads(f.read())

	config = {}
	config.update(configdata["common"])
	config.update(configdata[section])
	config["configuration"] = section
	return config


""" Dynamically import a method """
def import_from(module, name):
	import importlib
	module = __import__(module, fromlist=[name])
	return getattr(module, name)


""" Download a file using chunks to deal with large files. Disable default compression handling. """
def downloadFile(httpProxy,source,dest):
	import requests
	headers = {"Accept-Encoding": "identity"}
	r = requests.get(source, stream=True, proxies=httpProxy, headers=headers)
	with open(dest, 'wb') as f:
		for chunk in r.iter_content(chunk_size=1024):
			if chunk: # filter out keep-alive new chunks
				f.write(chunk)
				f.flush()


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


""" Return path of program if exists, http://stackoverflow.com/a/377028/426990 """
def which(program):
	import os
	def is_exe(fpath):
		return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

	fpath, fname = os.path.split(program)
	if fpath:
		if is_exe(program):
			return program
	else:
		for path in os.environ["PATH"].split(os.pathsep):
			path = path.strip('"')
			exe_file = os.path.join(path, program)
			if is_exe(exe_file):
				return exe_file

	return None


""" Quit when one of the programs is not found """
def checkPrograms(programlist):
	for p in programlist:
		if not which(p):
			raise RuntimeError( "executable does not exist: " + p )


""" return simple logger object """
def getLogger(application):
	import logging
	logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
	return logging.getLogger(application)


""" from for instance 2012-10-23T16:39:06Z """
def getTimestampFromZuluDT(dt):
	from datetime import datetime
	return int((datetime.strptime( dt, "%Y-%m-%dT%H:%M:%SZ") -  datetime(1970, 1, 1)).total_seconds())


""" pretty printer for debug """
def prettyPrint(data):
	import pprint
	pp = pprint.PrettyPrinter(indent=4)
	pp.pprint(data)
