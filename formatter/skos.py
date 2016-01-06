# -*- coding: utf-8 -*-
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import SKOS, RDF, XSD 


def makeSKOS(scheme,concepts):
	g = Graph()
	g.bind("skos", SKOS)

	""" scheme """
	subject = URIRef(scheme["identifier"])
	g.add((subject,RDF.type,SKOS.ConceptScheme))

	for top in scheme["topconcepts"]:
		g.add((subject,SKOS.hasTopConcept,URIRef(top)))

	""" terms """
	for topic in concepts:
		subject = URIRef(concepts[topic]["about"])
		g.add((subject,RDF.type,SKOS.Concept))
		g.add((subject,SKOS.prefLabel,Literal(concepts[topic]["label"], datatype=XSD.string )))
		g.add((subject,SKOS.inScheme,URIRef(scheme["identifier"])))

		for n in concepts[topic]["narrower"]:
			g.add((subject,SKOS.narrower,URIRef(n)))

		for b in concepts[topic]["broader"]:
			g.add((subject,SKOS.broader,URIRef(b)))


	return g.serialize(format="xml")
