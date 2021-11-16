#!/usr/bin/python
# -*- coding: utf8 -*-
"""SHACL validation utility"""

import logging
from urlparse import urljoin

import pylons
from rdflib.namespace import Namespace
import requests

LOGGER = logging.getLogger(__name__)

VALIDATE_ENDPOINT = 'validate'

SHACL = Namespace("http://www.w3.org/ns/shacl#")
DQV = Namespace("http://www.w3.org/ns/dqv#")
GOVDATA_MQA = Namespace("http://govdata.de/mqa/#")


class ShaclValidator(object):
    """Validates RDF graphs using the DCAT-AP.de SHACL validator service"""

    def __init__(self):
        self.validator_url, self.validator_profile = self._get_validator_config()

    def validate(self, rdf_graph, dataset_uri, dataset_org, rdf_format='text/turtle'):
        """Validates given RDF graph using the DCAT-AP.de SHACL validator service"""

        result = None
        if self.validator_url is not None and self.validator_profile is not None:
            body = {
                u'contentToValidate': rdf_graph,
                u'embeddingMethod': u'STRING',
                u'contentSyntax': rdf_format,
                u'validationType': self.validator_profile,
                u'reportQuery': self._get_report_query(dataset_uri, dataset_org)
            }

            try:
                req = requests.post(urljoin(self.validator_url, VALIDATE_ENDPOINT), json=body)

                if req.status_code == requests.codes.ok:
                    result = req.text
            except requests.exceptions.RequestException as ex:
                LOGGER.warn(u'Exception occurred while connecting to SHACL validator. Skip validating data ' \
                            u'with the SHACL validator, because validator is not available! Details: %s', ex)
        else:
            LOGGER.debug('Skip validating data with the SHACL validator, because validator is not available!')

        return result

    @staticmethod
    def _get_report_query(dataset_uri, owner_org):
        """Gets the report query for the SHACL validation request"""

        return u"""PREFIX sh: <{shacl}>
            PREFIX dqv: <{dqv}>
            PREFIX govdata: <{mqa}>
            CONSTRUCT {{
                ?report dqv:computedOn <{dataset_uri}> .
                ?report govdata:attributedTo '{owner_org}' .
                ?s ?p ?o .
            }} WHERE {{
                {{ ?report a sh:ValidationReport . }}
                UNION
                {{ ?s ?p ?o . }}
            }}""".format(shacl=SHACL, dqv=DQV, mqa=GOVDATA_MQA, dataset_uri=dataset_uri, owner_org=owner_org)

    @staticmethod
    def _get_validator_config():
        """Gets the URL to the SHACL validator API"""

        endpoint_base_url = pylons.config.get('ckanext.dcatde.shacl_validator.api_url')
        profile_type = pylons.config.get('ckanext.dcatde.shacl.validator.profile.type')

        if endpoint_base_url and not profile_type:
            LOGGER.warn(u'Invalid configuration of SHACL validator. Profile type is missing! SHACL ' \
                        u'validation support is deactivated.')
        elif profile_type and not endpoint_base_url:
            LOGGER.warn(u'Invalid configuration of SHACL validator. Base URL is missing! SHACL validation ' \
                        u'support is deactivated.')
        elif endpoint_base_url and profile_type:
            LOGGER.info(u'Found SHACL validator URL and validation profile in config. SHACL validation ' \
                        u'support is activated.')
        else:
            LOGGER.info(u'Did not find configurations for SHACL validator. SHACL validaton support is ' \
                        u'deactivated.')
        return endpoint_base_url, profile_type
