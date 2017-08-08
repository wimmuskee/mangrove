# -*- coding: utf-8 -*-
"""
This module contains text processing
functions for the mangrove crawler.

Wim Muskee, 2013-2017
wimmuskee@gmail.com

License: GPL-3
"""

import os
from collections import defaultdict
from nltk import tokenize
from math import ceil

class TextProcessor:
	def __init__(self,text,locale,calculator=None):
		self.text = text
		self.locale = locale
		self.stopwords = self.getStopwords()
		
		if calculator == "kpc":
			from readability_score.calculators.nl import kpc
			self.calculator = kpc.KPC(self.text,locale)
		else:
			from readability_score.calculators import fleschkincaid
			self.calculator = fleschkincaid.FleschKincaid(self.text,locale)


	# try stopwords in mangrove root with locale, default to stopwords.txt
	def getStopwords(self):
		if os.path.exists("stopwords_" + self.locale + ".txt"):
			sw_file = "stopwords_" + self.locale + ".txt"
		elif os.path.exists("stopwords.txt"):
			sw_file = "stopwords.txt"
		else:
			return []
		
		with open(sw_file, "r") as f:
			stopwords = [w.strip() for w in f.readlines()]

		return stopwords


	def getKeywords(self):
		""" not using work_tokenize because of unicode characters """
		words = tokenize.wordpunct_tokenize(self.text)
		filtered_words = [w.lower() for w in words if not w.lower() in self.stopwords]
		include_threshold = round(self.calculator.scores['word_count'] * 0.003)

		""" collect word statistics """
		counts = defaultdict(int) 
		for word in filtered_words:
			if word.isalpha() and len(word) > 1:
				counts[word] += 1

		""" only most occuring; filter words based on threshhold """
		keys = defaultdict(int)
		for word,count in counts.items():
			if count > include_threshold:
				keys[word] = count

		keywords = sorted(keys, key=keys.get, reverse=True)
		return keywords[:15]


	def getReadingTime(self,wordcount,min_age):
		""" Get a reading time based on the wordcount, minimal age calculation and partly
		research based algorithm for age based reading speed (words/minute). """
		readspeed = { 6: 28, 7: 56, 8: 73, 9: 84, 10: 90, 11: 95, 12: 96, 13: 113, 14: 130, 15: 147, 16: 164, 17: 181, 18: 198 }
		if int(min_age) in readspeed:
			return int(ceil(wordcount/readspeed[int(min_age)] * 60))
		else:
			return int(ceil(wordcount/215 * 60))
