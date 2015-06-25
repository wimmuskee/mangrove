#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
The basic execution script for the mangrove purger

Wim Muskee, 2015
wimmuskee@gmail.com

License: GPL-3
"""

import argparse
import os.path
from sys import exit, exc_info
from mangrove_crawler import common

parser = argparse.ArgumentParser(description='Purger for the Source to LOM project.')
parser.add_argument('-s', '--source', nargs=1, help='Data provider', metavar='datasource', dest='source')
parser.add_argument('-m', '--method', nargs=1, help='Purging method; sync, record id, or file', metavar='method', dest='method')
parser.add_argument('-c', '--config', nargs=1, help='Config file', metavar='configfile', dest='configfile')

args = parser.parse_args()
configfile = "mangrove-crawler.cfg"

if args.configfile:
	configfile = args.configfile[0]
	if not os.path.isfile(configfile):
		parser.error('Config file not found: ' + configfile)

if not args.source:
	parser.error('Input a valid source')

if not args.method:
	parser.error('Input a valid method')

source = args.source[0]
method = args.method[0]


""" get common config """
try:
	config = common.getConfig(configfile,"common")
except:
	parser.error('No common config defined')

""" merge config for source """
try:
	config.update(common.getConfig(configfile,source))
except:
	parser.error('Invalid source config: ' + source)


""" load and start purger """
try:
	purger = common.import_from('mangrove_crawler.interfaces.' + config['module'], 'purger')
	Purger = purger.Purger(config)
except:
	print("Cannot load module for source: " + source)
	print "Unexpected error: ", exc_info()[0]
	print exc_info()[1]
	exit()

""" starting purge process """
print "Purging: " + source + " by " + method
Purger.purge(method)

