# -*- coding: utf-8 -*-

""" Return UNIX timestamp from yyymmddhhmmss """
def makeTimestamp(timestamp):
	from time import mktime
	from datetime import datetime
	year = int(timestamp[0:4])
	month = int(timestamp[4:6])
	day = int(timestamp[6:8])
	hour = int(timestamp[8:10])
	minute = int(timestamp[10:12])
	second = int(timestamp[12:14])

	dt = datetime( year, month, day, hour, minute, second )
	return int(mktime(dt.timetuple()))
