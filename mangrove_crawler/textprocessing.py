# -*- coding: utf-8 -*-
"""
This module contains text processing
functions for the mangrove crawler.

Wim Muskee, 2013
wimmuskee@gmail.com

License: GPL-3
"""

def getStopwords():
	from nltk.corpus import stopwords
	ignored_words = stopwords.words('dutch')
	# voorzetsel
	ignored_words.extend(['per', 'tijdens', 'ten', 'ter', 'tussen', 'volgens', 'sinds', 'via'])
	# bijwoord
	ignored_words.extend(['soms', 'steeds', 'echter', 'wel', 'vaak', 'bijvoorbeeld', 'vooral', 'meestal', 'ongeveer', 'mogelijk', 'waarbij', 'terug'])
	# voegwoord
	ignored_words.extend(['zoals'])
	# telwoord
	ignored_words.extend(['alle', 'eerste', 'tweede', 'enkele', 'sommige', 'vele', 'beide', 'twee'])
	# werkwoord
	ignored_words.extend(['werden', 'gebruikt', 'ligt', 'staat', 'staan', 'komt', 'kwam', 'komen', 'bestaat', 'kreeg', 'ging', 'gaat', 'gaan'])
	# voornaamwoord
	ignored_words.extend(['elkaar', 'eigen'])
	# other
	ignored_words.extend(['hierna', 'jaar', 'jaren', 'later', 'verschillend', 'km', 'ii', 'chr', 'alleen', 'af', 'waar', 'verschillende' ])

	return ignored_words
