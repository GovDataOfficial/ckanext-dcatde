#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Query templates for SPARQL requests
"""
# pylint: disable=pointless-string-statement

from ckanext.dcatde.validation.shacl_validation import DQV, GOVDATA_MQA

'''
When formatting this query:
Format %(uri)s with the URI of the dataset
'''
DELETE_DATASET_BY_URI_SPARQL_QUERY = u"""DELETE { ?s ?p ?o }
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
DELETE_VALIDATION_REPORT_BY_URI_SPARQL_QUERY = u"""PREFIX dqv: <{dqv}>
            PREFIX govdata: <{mqa}>
            DELETE {{ ?s ?p ?o }}
            WHERE {{
              ?report dqv:computedOn <%(uri)s> .
              ?report (<>|!<>)* ?s .
              ?s (<>|!<>)* ?o .
              ?s ?p ?o
            }}""".format(dqv=DQV, mqa=GOVDATA_MQA)

'''
When formatting this query:
Format %(uri)s with the URI
'''
DELETE_DATASET_FROM_HARVEST_INFO_QUERY = u"""DELETE { ?s ?p ?o }
            WHERE {
              ?s ?p ?o
              FILTER ( ?s = <%(uri)s> )
            }"""

'''
When formatting this query:
Format %(uri)s with the URI of the dataset
'''
GET_DATASET_BY_URI_SPARQL_QUERY = u"""SELECT ?s ?p ?o
                            WHERE {
                              <%(uri)s> (<>|!<>)* ?s .
                              ?s ?p ?o }"""

'''
When formatting this query:
Format %(owner_org_or_source_id)s with the organization id of the dataset
'''
GET_URIS_FROM_HARVEST_INFO_QUERY = u"""SELECT ?s ?p ?o
                                    WHERE {
                                        ?s ?p '%(owner_org_or_source_id)s'
                                    }"""
