# -*- coding: utf-8 -*-

import json
import requests
from aniso8601 import parse_duration

BASEURL = "http://backstage-api.npo.nl/v0/"
MAXRESULTS = 10

def getResultPage(httpProxy,collection_id,fromdate,token):
	SEARCH_DATA = {
		"sort": "date",
		"order": "asc",
		"size": MAXRESULTS,
		"from": token,
		"filters": { "date": { "from": fromdate } }
	}
	
	session = requests.session()
	response = session.post( BASEURL + collection_id + "/search", data=json.dumps(SEARCH_DATA) )

	videos = dict()
	meta = dict()

	meta["token"] = int(token) + MAXRESULTS
	meta["total"] = response.json()['hits']['total']

	# if there is no metarecord, the object is not available
	for item in response.json()['hits']['hits']:
		video = {
			"id": "",
			"title": "",
			"description": "",
			"location":"",
			"duration":0,
			"keywords": [] }
		metarecord = getMetaRecord(httpProxy,item["_id"])
		
		if not metarecord:
			continue
		else:
			video.update(metarecord)
		
		video["id"] = item["_id"]
		video["title"] = item["_source"]["title"]
		if "description" in item["_source"]:
			video["description"] = item["_source"]["description"]
		if "keywords" in item["_source"]:
			video["keywords"] = item["_source"]["keywords"]
		video["location"] = item["_source"]["meta"]["original_object_urls"]["html"]
		video["typicalageranges"] = getTypicalAgeRanges( item["_source"]["typical_age_ranges"])
		#video.update(getMetaRecord(httpProxy,item["_id"]))
		videos[item["_id"]] = video
	
	return { "meta" : meta, "videos":videos }


def getMetaRecord(httpProxy,id):
	record = dict()
	data = getMetaData(httpProxy,id)
	
	if getVideoAvailableStatus(data):
		#record["publishdate"] = data["publicatie_start"]
		record["authors"] = data["authors"]
		if data["tijdsduur"]:
			record["duration"] = getDuration( data["tijdsduur"] )
	
	return record


""" for 00:07:42 duration format, return seconds """
def getDuration(tijdsduur):
	timelist = tijdsduur.split(":")
	durationstring = "PT" + timelist[0] + "H" + timelist[1] + "M" + timelist[2] + "S"
	return parse_duration( durationstring ).seconds


""" make string format from input """
def getTypicalAgeRanges(agerangelist):
	newrangelist = []
	for ageranges in agerangelist:
		newrangelist.append( ageranges["from_age"] + "-" + ageranges["till_age"] )
	
	return newrangelist


""" get the metadata record for a video id """
def getMetaData(httpProxy,video_id):
	URL = BASEURL + "metadata/" + video_id
	response = requests.get(URL, stream=True, proxies=httpProxy)
	data = response.json()
	return data


""" testing availaility through metadata collection """
def getVideoAvailableStatus(data):
	if "error" in data:
		return False
	else:
		from datetime import datetime
		
		if "publicatie_eind" in data:
			# ignore timezone, can do this better in python3
			pub_end = data["publicatie_eind"][0:19]
			if int(datetime.now().strftime("%s")) > int(datetime.strptime(pub_end, '%Y-%m-%dT%H:%M:%S').strftime("%s")):
				return False
		
		return True
