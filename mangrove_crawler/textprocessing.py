# -*- coding: utf-8 -*-
"""
This module contains text processing
functions for the mangrove crawler.

Wim Muskee, 2013-2015
wimmuskee@gmail.com

License: GPL-3
"""

def getStopwords(httpProxy):
	words = []
	words.extend(getWordList(httpProxy,"http://lexicon.edurep.nl/woordenlijst/bijwoord.txt"))
	words.extend(getWordList(httpProxy,"http://lexicon.edurep.nl/woordenlijst/lidwoord.txt"))
	words.extend(getWordList(httpProxy,"http://lexicon.edurep.nl/woordenlijst/voegwoord.txt"))
	words.extend(getWordList(httpProxy,"http://lexicon.edurep.nl/woordenlijst/voornaamwoord.txt"))
	words.extend(getWordList(httpProxy,"http://lexicon.edurep.nl/woordenlijst/voorzetsel.txt"))
	words.extend(getWordList(httpProxy,"http://lexicon.edurep.nl/woordenlijst/werkwoord-vervoegingen.txt"))
	return words


""" Asume text file with word each line """
def getWordList(httpProxy,uri):
	import requests
	words = []
	r = requests.get(uri, proxies=httpProxy)
	for line in r.iter_lines():
		words.append( line )

	return words