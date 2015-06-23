# -*- coding: utf-8 -*-

from apiclient.discovery import build
from aniso8601 import parse_duration

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
MAXRESULTS = 10

def getChannelPage(httpProxy,developer_key,channel_id,fromts,token=""):
	# https://developers.google.com/youtube/v3/docs/search/list
	youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=developer_key, http=httpProxy)

	response = youtube.search().list(
		channelId=channel_id,
		part="id,snippet",
		pageToken=token,
		publishedAfter=fromts,
		maxResults=MAXRESULTS
	).execute()

	videos = dict()
	meta = dict()

	if "nextPageToken" in response.keys():
		meta["token"] = response["nextPageToken"]
	else:
		meta["token"] = ""

	for item in response.get("items", []):
		if item["id"]["kind"] == "youtube#video":
			video = dict()
			video["channel_id"] = channel_id
			video['title'] = item["snippet"]["title"]
			video["youtube_id"] = item["id"]["videoId"]
			video["description"] = item["snippet"]["description"]
			video["thumbnail"] = item["snippet"]["thumbnails"]["default"]["url"]
			video["publishdate"] = item["snippet"]["publishedAt"]
			videos[item["id"]["videoId"]] = video

	videodetails = getVideoDetails(httpProxy,developer_key,",".join(videos.keys()))
	
	for vid in videodetails.keys():
		videos[vid].update(videodetails[vid])

	return { "meta" : meta, "videos":videos }


def getVideoDetails(httpProxy,developer_key,video_ids):
	# https://developers.google.com/youtube/v3/docs/videos/list
	youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=developer_key, http=httpProxy)

	response = youtube.videos().list(
		id=video_ids,
		part="contentDetails,status",
		maxResults=MAXRESULTS
	).execute()
	
	videos = dict()
	
	for item in response.get("items", []):
		video = dict()
		video["duration"] = parse_duration( item["contentDetails"]["duration"] ).seconds
		video["license"] = item["status"]["license"]
		video["embed"] = item["status"]["embeddable"]
		video["uploadStatus"] = item["status"]["uploadStatus"]
		video["privacyStatus"] = item["status"]["privacyStatus"]
		videos[item["id"]] = video

	return videos


def getChannelInfo(httpProxy,developer_key,username):
	# https://developers.google.com/youtube/v3/docs/channels/list
	youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=developer_key, http=httpProxy)
	
	response = youtube.channels().list(
		forUsername=username,
		part="id,snippet,contentDetails,statistics",
		maxResults=MAXRESULTS
	).execute()
	
	return response["items"][0]


def getVideoAvailableStatus(developer_key,youtube_id):
	# similar to getVideoDetails, just getting the id and check nr of results.
	# just checking on id, does not cost quota
	youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=developer_key, http=httpProxy)

	response = youtube.videos().list(
		id=youtube_id,
		part="id",
		maxResults=1
	).execute()
	
	if response["pageInfo"]["totalResults"] == 0:
		return False
	else:
		return True
