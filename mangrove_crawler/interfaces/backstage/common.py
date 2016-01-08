# -*- coding: utf-8 -*-

import json
import requests
from datetime import datetime

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

	""" get videos + metadata """
	for item in response.json()['hits']['hits']:
		videometadata = getMetaData(httpProxy,item["_id"])

		# if there is no metarecord, the object is not available
		if not getVideoAvailableStatus(videometadata):
			continue
		
		video = { "item": item, "metadata": videometadata }
		videos[item["_id"]] = video

	return { "meta" : meta, "videos":videos }


""" for 00:07:42 duration format, return duration string """
def getDuration(tijdsduur):
	timelist = tijdsduur.split(":")
	durationstring = "PT" + timelist[0] + "H" + timelist[1] + "M" + timelist[2] + "S"
	return durationstring


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
		if "publicatie_eind" in data:
			# ignore timezone, can do this better in python3
			pub_end = data["publicatie_eind"][0:19]
			if int(datetime.now().strftime("%s")) > int(datetime.strptime(pub_end, '%Y-%m-%dT%H:%M:%S').strftime("%s")):
				return False
		
		return True
