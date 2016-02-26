#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import os.path
from mangrove_crawler import admin, common

parser = argparse.ArgumentParser(description='Mangrove admin tool.')
commands = parser.add_mutually_exclusive_group()
commands.add_argument('-l', '--list', help="list collections", action="store_true")
commands.add_argument('-g', '--get', help="get record information", nargs=1, dest='record')
parser.add_argument('-c', '--config', nargs=1, help='Config file', metavar='configfile', dest='configfile')

args = parser.parse_args()
configfile = "mangrove-crawler.cfg"

if args.configfile:
	configfile = args.configfile[0]
	if not os.path.isfile(configfile):
		parser.error('Config file not found: ' + configfile)

""" get common config """
try:
	config = common.getConfig(configfile,"common")
except:
	parser.error('No common config defined')


Admin = admin.Admin(config)


if args.list:
	Admin.getCollections()
elif args.record:
	record = args.record[0]
	Admin.getRecord(record)
