#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Query templates for SPARQL requests
"""

'''
When formatting this query:
Format %(uri)s with the URI of the dataset
'''
DELETE_DATASET_BY_URI_SPARQL_QUERY = """DELETE { ?s ?p ?o }
            WHERE {
              <%(uri)s> (<>|!<>)* ?s .
              ?s ?p ?o
                  MINUS { <%(uri)s> <http://purl.org/dc/terms/publisher> ?s .
                    ?s ?p ?o
                    FILTER (!isBlank(?s)) }
            }"""

'''
When formatting this query:
Format %(uri)s with the URI of the dataset
'''
GET_DATASET_BY_URI_SPARQL_QUERY = """SELECT ?s ?p ?o
                            WHERE {
                              <%(uri)s> (<>|!<>)* ?s .
                              ?s ?p ?o }"""
