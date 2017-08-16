# -*- coding: utf-8 -*-
# Mediawiki records are the last to be updated to the current Mangrove design.
# Most prominent is making the xml records at local harvest time.
# For the migration, we have to do this here, based on the old database.
# Keep the old database in the source config, and generic mangrove db in the common config.

# copy script to application root and execute:
# python2 201708-migrate-mediawiki-to-updated-design.py -s wikikids -d wikilom -u "http://wikikids.nl/"


import argparse
from mangrove_libs import common
from mangrove_libs.textprocessing import TextProcessor
from storage.mysql import Database
from storage.filesystem import Filesystem
from formatter.nllom import makeLOM, getEmptyLomDict, formatDurationFromSeconds
from formatter.oaidc import makeOAIDC, getEmptyOaidcDict
import datetime

parser = argparse.ArgumentParser(description='migration script for mediawiki records')
parser.add_argument('-s', '--source', nargs=1, help='Mediawiki Data provider', metavar='datasource', dest='source')
parser.add_argument('-u', '--host', nargs=1, help='source location host', metavar='host', dest='host')

args = parser.parse_args()

if args.source:
	source = args.source[0]
else:
	parser.error('Input a valid source')

if args.host:
	host = args.host[0]
else:
	parser.error('Provide the host with which to prefix the database url_title')

# load config
configfile = "mangrove-crawler.cfg"
config = common.getConfig(configfile,"common")
config["configuration"] = source
oldconfig = common.getConfig(configfile,source)

# set classes
DB = Database(config["db_host"],config["db_user"],config["db_passwd"],config["db_name"])
DB.setCollectionInfo(source)
OLDDB = Database(oldconfig["db_host"],oldconfig["db_user"],oldconfig["db_passwd"],oldconfig["db_name"])
t = TextProcessor("","nl_NL")
FS = Filesystem(config)

# retrieve old data
c = OLDDB.DB.cursor()
query = "SELECT * FROM %s" % source
c.execute(query)
recorddata = c.fetchall()
c.close()

for record in recorddata:
	# get keywords
	keywords = []
	c = OLDDB.DB.cursor()
	query = "SELECT keyword FROM %s AS wkw LEFT JOIN keywords AS kw ON wkw.keyword_id = kw.id WHERE page_id = %s" % (source + "_article_has_keywords", record["page_id"])
	c.execute(query)
	kwdata = c.fetchall()
	c.close()
	for kw in kwdata:
		keywords.append(kw["keyword"])

	r = getEmptyLomDict()
	r["publisher"] = "Wikikids"
	r["cost"] = "no"
	r["language"] = "nl"
	r["aggregationlevel"] = "2"
	r["metalanguage"] = "nl"
	r["structure"] = "hierarchical"
	r["format"] = "text/html"
	r["intendedenduserrole"] = "learner"
	r["learningresourcetype"] = "informatiebron"
	r["interactivitytype"] = "expositive"
	r["context"] = ["PO"]
	r["typicalagerange"] = "10-12"
	r["copyrightandotherrestrictions"] = "cc-by-sa-30"
	r["identifier"] = [ { "catalog": "URI", "value": host + record["url_title"] } ]
	r["title"] = record["title"]
	r["description"] = record["description"]
	r["keywords"] = keywords
	r["version"] = record["version"]
	r["publishdate"] = datetime.datetime.fromtimestamp(int(record["updated"])).strftime('%Y-%m-%dT%H:%M:%SZ')
	r["location"] = host + record["url_title"]
	r["isversionof"] =  host + "/index.php?title=" + record["url_title"] + "&oldid=" + str(record["lastrev_id"])
	r["typicallearningtime"] = formatDurationFromSeconds(t.getReadingTime(record["words"],10))
	# not copying difficulty, for now, hard to implement at future harvest level (generically)	
	lom = makeLOM(r)

	o = getEmptyOaidcDict()
	o["title"] = r["title"]
	o["description"] = r["description"]
	o["subject"] = r["keywords"]
	o["publisher"] = r["publisher"]
	o["format"] = r["format"]
	o["identifier"] = r["location"]
	o["language"] = r["language"]
	o["rights"] = r["copyright"]
	oaidc = makeOAIDC(o)

	# get oaidata for record
	c = OLDDB.DB.cursor()
	query = "SELECT updated, deleted FROM oairecords WHERE identifier = %s"
	c.execute(query,(record["identifier"],))
	oaidata = c.fetchone()
	c.close()

	# store files
	FS.storeRecord("lom",record["identifier"],lom)
	FS.storeRecord("oaidc",record["identifier"],oaidc)

	# store oairecord
	c = DB.DB.cursor()
	row = DB.getRecordByOriginalId(record["url_title"])
	if row:
		query = "UPDATE oairecords SET lom=%s, oaidc=%s WHERE original_id=%s"
		c.execute(query,(lom,oaidc,record["url_title"]))
	else:
		query = "INSERT INTO oairecords (identifier,original_id,collection_id,updated,deleted,lom,oaidc) VALUES ( %s, %s, %s, %s, %s, %s, %s )"
		c.execute(query, (record["identifier"],record["url_title"],DB.collection_id,oaidata["updated"],oaidata["deleted"],lom,oaidc))

	DB.DB.commit()
	c.close()
