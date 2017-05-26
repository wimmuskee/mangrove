#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import argparse
import os.path
from mangrove_libs import admin, common

parser = argparse.ArgumentParser(description='Mangrove admin tool.')
parser.add_argument('-c', '--config', nargs=1, help='config file', metavar='configfile', dest='configfile')
listcollections = parser.add_argument_group('collections list')
listcollections.add_argument('-l', '--list', help="list collections", action="store_true")
addcollection = parser.add_argument_group('collection add')
addcollection.add_argument('-a', '--add', help="add collection", nargs=1, metavar='collection name', dest='add')
getrecord = parser.add_argument_group('record info')
getrecord.add_argument('-g', '--get', help="get record information", nargs=1, metavar='record identifier', dest='get')
getrecord.add_argument('-f', '--field', help="return xml field", nargs=1, metavar='lom or oaidc', dest='field', choices=['lom','oaidc'])
delrecord = parser.add_argument_group('record delete')
delrecord.add_argument('-d', '--delete', help="delete record", nargs=1, metavar='record identifier', dest='delete')
stats = parser.add_argument_group('statistics')
stats.add_argument('-s', '--stats', help="record statistics", action="store_true")


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


try:
	Admin = admin.Admin(config)

	if args.list:
		Admin.getCollections()
	elif args.stats:
		Admin.getStats()
	elif args.add:
		collection = args.add[0]
		Admin.addCollection(collection)
	elif args.get:
		record = args.get[0]
		field = None
		if args.field:
			field = args.field[0]
		Admin.getRecord(record,field)
	elif args.delete:
		record = args.delete[0]
		Admin.deleteRecord(record)
except Exception as err:
	print(str(err))
