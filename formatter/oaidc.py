# -*- coding: utf-8 -*-
from lxml import etree

oaidcns = "{http://www.openarchives.org/OAI/2.0/oai_dc/}"
dcns = "{http://purl.org/dc/elements/1.1/}"

def getEmptyOaidcDict():
	oaidcdict = {
		"title": "",
		"description": "",
		"subject": [],
		"publisher": "",
		"format": "",
		"identifier": "",
		"language": "",
		"rights": "" }
	return oaidcdict


def makeOAIDC(oaidcdict):
	oaidc = etree.Element(oaidcns + 'dc', nsmap={"oai_dc": 'http://www.openarchives.org/OAI/2.0/oai_dc/', "dc": "http://purl.org/dc/elements/1.1/", "xsi": "http://www.w3.org/2001/XMLSchema-instance"})
	
	if oaidcdict["title"]:
		oaidc.append(makeElement("title",oaidcdict["title"]))

	if oaidcdict["description"]:
		oaidc.append(makeElement("description",oaidcdict["description"]))

	if oaidcdict["subject"]:
		for s in oaidcdict["subject"]:
			oaidc.append(makeElement("subject",s))

	if oaidcdict["publisher"]:
		oaidc.append(makeElement("publisher",oaidcdict["publisher"]))

	if oaidcdict["format"]:
		oaidc.append(makeElement("format",oaidcdict["format"]))

	if oaidcdict["identifier"]:
		oaidc.append(makeElement("identifier",oaidcdict["identifier"]))

	if oaidcdict["language"]:
		oaidc.append(makeElement("language",oaidcdict["language"]))

	if oaidcdict["rights"]:
		oaidc.append(makeElement("rights",oaidcdict["rights"]))

	s = etree.tostring(oaidc, pretty_print=True)
	return s


def makeElement(element,value):
	e = etree.Element(dcns + element)
	e.text = value
	return e
