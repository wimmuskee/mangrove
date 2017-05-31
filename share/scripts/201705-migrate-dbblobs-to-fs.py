# -*- coding: utf-8 -*-
# used to migrate db lom and oaidc blobs to fs files
# 2017

import argparse
from mangrove_libs import common
from mangrove_libs.interface import Interface

parser = argparse.ArgumentParser(description='migration script for filestore')
parser.add_argument('-s', '--source', nargs=1, help='Data provider', metavar='datasource', dest='source')
args = parser.parse_args()

if args.source:
	source = args.source[0]
else:
	parser.error('Input a valid source')

configfile = "mangrove-crawler.cfg"
config = common.getConfig(configfile,"common")
config["configuration"] = source

iface = Interface(config)
records = iface.DB.getAll()

for record in records:
	iface.FS.storeRecord("lom",record["identifier"],record["lom"])
	iface.FS.storeRecord("oaidc",record["identifier"],record["oaidc"])
