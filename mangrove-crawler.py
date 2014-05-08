#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
The basic execution script for the mangrove crawler

Wim Muskee, 2013
wimmuskee@gmail.com

License: GPL-3
"""

import argparse
import os.path
from sys import exit, exc_info
from mangrove_crawler import common

parser = argparse.ArgumentParser(description='Crawler for the Source to LOM project.')
parser.add_argument('-s', '--source', nargs=1, help='Data provider', metavar='datasource', dest='source')
parser.add_argument('-p', '--part', nargs=1, help='Identifiable subset of provider', metavar='subset', dest='part')
parser.add_argument('-c', '--config', nargs=1, help='Config file', metavar='configfile', dest='configfile')

args = parser.parse_args()

if args.configfile:
	configfile = args.configfile[0]
	if not os.path.isfile(configfile):
		parser.error('Invalid config file: ' + configfile)
else:
	parser.error('You have to specify a config file')


if args.source:
	source = args.source[0]

	if args.part:
		part = args.part[0]
	else:
		part = ""

	""" get config for source """
	try:
		config = common.getConfig(configfile,source)
	except:
		parser.error('Invalid source: ' + source)

	""" load and start harvester """
	try:
		harvester = common.import_from('mangrove_crawler.interfaces.' + config['module'], 'harvester')
		Harvester = harvester.Harvester(config)
	except:
		print("Cannot load module for source: " + source)
		print "Unexpected error: ", exc_info()[0]
		exit()

	""" starting harvest process """
	print "Harvesting: " + source
	Harvester.harvest(part)
else:
	parser.error('Input a valid source')
