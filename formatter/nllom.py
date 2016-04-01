# -*- coding: utf-8 -*-
from lxml import etree

vocabdict = { "aggregationlevel": "LOMv1.0",
	"role": "LOMv1.0",
	"learningresourcetype": "http://purl.edustandaard.nl/vdex_learningresourcetype_czp_20060628.xml",
	"context": "http://purl.edustandaard.nl/vdex_context_czp_20060628.xml",
	"intendedenduserrole": "LOMv1.0",
	"cost": "LOMv1.0",
	"copyrightandotherrestrictions": "http://purl.edustandaard.nl/copyrightsandotherrestrictions_nllom_20131202",
	"kind": "http://purl.edustandaard.nl/relation_kind_nllom_20131211",
	"purpose": "LOMv1.0" }

metalang = "en"
xmlns = "{http://www.imsglobal.org/xsd/imsmd_v1p2}"

# example classification
# [ [{ "id": "c7942bb1-bf4f-409f-bc98-9a0b02f175dc", "value": "VMBO" }, { "id": "12e85a55-b3ae-4e7f-a2a0-d645f4c573bf", "value": "VMBO KL 1" }] ]
def getEmptyLomDict():
	lomdict = {
		"identifier": [],
		"title": "",
		"description": "",
		"keywords": [], 
		"language": "",
		"aggregationlevel": "",
		"publisher": "",
		"publishdate": "",
		"author": [],
		"metalanguage": "",
		"format": "",
		"location": "",
		"context": [],
		"learningresourcetype": "",
		"intendedenduserrole": "",
		"typicalagerange": "",
		"duration": "",
		"cost": "",
		"copyright": "",
		"embed": "",
		"thumbnail": "",
		"discipline": [],
		"educationallevel": [] }
	return lomdict


def makeLOM(lomdict):
	lom = etree.Element(xmlns + 'lom', nsmap={"lom": 'http://www.imsglobal.org/xsd/imsmd_v1p2', "xsi": "http://www.w3.org/2001/XMLSchema-instance"})
	general = etree.Element(xmlns + "general")
	lifecycle = etree.Element(xmlns + "lifecycle")
	metametadata = etree.Element(xmlns + "metametadata")
	technical = etree.Element(xmlns + "technical")
	educational = etree.Element(xmlns + "educational")
	rights = etree.Element(xmlns + "rights")

	if lomdict["metalanguage"]:
		global metalang
		metalang = lomdict["metalanguage"]
		

	if lomdict["title"]:
		general.append(makeLangstring("title", lomdict["title"]))

	if lomdict["identifier"]:
		for identifier in lomdict["identifier"]:
			general.append(makeCatalogEntry(identifier["catalog"],identifier["value"]))

	if lomdict["language"]:
		general.append(makeElement("language", lomdict["language"]))

	if lomdict["description"]:
		general.append(makeLangstring("description", lomdict["description"]))

	if lomdict["keywords"]:
		for kw in lomdict["keywords"]:
			general.append(makeLangstring("keyword", kw))

	if lomdict["aggregationlevel"]:
		general.append(makeVocab("aggregationlevel", lomdict["aggregationlevel"]))

	if lomdict["publisher"]:
		lifecycle.append(makeContribute("publisher",lomdict["publisher"],lomdict["publishdate"]))

	if lomdict["author"]:
		for author in lomdict["author"]:
			lifecycle.append(makeContribute("author",makeVcard(author)))

	metametadata.append(makeElement("metadatascheme","LOMv1.0"))
	metametadata.append(makeElement("metadatascheme","nl_lom_v1p0"))
	
	if lomdict["metalanguage"]:
		metametadata.append(makeElement("language", lomdict["metalanguage"]))

	if lomdict["format"]:
		technical.append(makeElement("format",lomdict["format"]))

	if lomdict["location"]:
		technical.append(makeElement("location",lomdict["location"]))

	if lomdict["duration"]:
		technical.append(makeDuration("duration",lomdict["duration"]))

	if lomdict["learningresourcetype"]:
		educational.append(makeVocab("learningresourcetype",lomdict["learningresourcetype"]))

	if lomdict["intendedenduserrole"]:
		educational.append(makeVocab("intendedenduserrole",lomdict["intendedenduserrole"]))

	for c in lomdict["context"]:
		educational.append(makeVocab("context",c))

	if lomdict["typicalagerange"]:
		educational.append(makeElement("typicalagerange",lomdict["typicalagerange"]))

	if lomdict["cost"]:
		rights.append(makeVocab("cost", lomdict["cost"]))

	if lomdict["copyright"]:
		if lomdict["copyright"] != "no" and lomdict["copyright"][:2] != "cc":
			rights.append(makeVocab("copyrightandotherrestrictions", "yes"))
			rights.append(makeLangstring("description", lomdict["copyright"]))
		else:
			rights.append(makeVocab("copyrightandotherrestrictions", lomdict["copyright"]))

	lom.append(general)
	lom.append(lifecycle)
	lom.append(metametadata)
	lom.append(technical)
	lom.append(educational)
	lom.append(rights)

	if lomdict["embed"]:
		lom.append(makeRelation("embed", lomdict["embed"]))

	if lomdict["thumbnail"]:
		lom.append(makeRelation("thumbnail", lomdict["thumbnail"]))

	# multiple educationallevel classifications
	for levels in lomdict["educationallevel"]:
		lom.append(makeObkClassification("educational level",levels))

	for disciplines in lomdict["discipline"]:
		lom.append(makeObkClassification("discipline",disciplines))

	s = etree.tostring(lom, pretty_print=True)
	return s


def makeLangstring(element,value,language=None):
	if not language:
		language = metalang

	e = etree.Element(xmlns + element)
	lang = etree.Element(xmlns + "langstring", {'{http://www.w3.org/XML/1998/namespace}lang':language})
	lang.text = value
	e.append(lang)
	return e

def makeVocab(element,value):
	e = etree.Element(xmlns + element)
	source = etree.Element(xmlns + "source")
	val = etree.Element(xmlns + "value")
	srclang = etree.Element(xmlns + "langstring", {'{http://www.w3.org/XML/1998/namespace}lang':'x-none'})
	srclang.text = vocabdict[element]
	vallang = etree.Element(xmlns + "langstring", {'{http://www.w3.org/XML/1998/namespace}lang':'x-none'})
	vallang.text = value
	source.append(srclang)
	val.append(vallang)
	e.append(source)
	e.append(val)
	return e

def makeContribute(role,vcard,date=""):
	e = etree.Element(xmlns + "contribute")
	e.append(makeVocab("role",role))
	entity = etree.Element(xmlns + "centity")
	ventity = etree.Element(xmlns + "vcard")
	ventity.text = vcard
	entity.append(ventity)
	e.append(entity)
	if date:
		d = etree.Element(xmlns + "date")
		dt = etree.Element(xmlns + "datetime")
		dt.text = date
		d.append(dt)
		e.append(d)
	return e

def makeVcard(entry):
	vcard = u"BEGIN:VCARD\u000AVERSION:3.0\u000A"
	vcard = vcard + u"FN:" + entry["fn"] + u"\u000A"
	if "url" in entry:
		vcard = vcard + u"URL:" + entry["url"] + u"\u000A"
	vcard = vcard + u"END:VCARD"
	return vcard

def makeDuration(element,value):
	e = etree.Element(xmlns + element)
	e.append(makeElement("datetime",value))
	return e

def makeElement(element,value):
	e = etree.Element(xmlns + element)
	e.text = value
	return e

def makeCatalogEntry(key,identifier):
	e = etree.Element(xmlns + "catalogentry")
	catalog = etree.Element(xmlns + "catalog")
	catalog.text = key
	e.append(catalog)
	e.append(makeLangstring("entry",identifier,"x-none"))
	return e

def makeRelation(kind,identifier):
	e = etree.Element(xmlns + "relation")
	e.append(makeVocab("kind",kind))
	resource = etree.Element(xmlns + "resource")
	resource.append(makeCatalogEntry("URI",identifier))
	e.append(resource)
	return e

def makeObkClassification(purposevalue,tps):
	e = etree.Element(xmlns + "classification")
	e.append(makeVocab("purpose",purposevalue))
	for txs in tps:
		taxonpath = etree.Element(xmlns + "taxonpath")
		taxonpath.append(makeLangstring("source","http://purl.edustandaard.nl/begrippenkader","x-none"))
		for t in txs:
			taxon = etree.Element(xmlns + "taxon")
			id = etree.Element(xmlns + "id")
			id.text = t["id"]
			taxon.append(id)
			taxon.append(makeLangstring("entry", t["value"], "nl"))
			taxonpath.append(taxon)
			e.append(taxonpath)
	return e

def formatDurationFromSeconds(seconds):
	return "PT" + str(seconds) + "S"
