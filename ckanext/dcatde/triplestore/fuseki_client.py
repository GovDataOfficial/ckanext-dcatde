#!/usr/bin/python
# -*- coding: utf8 -*-
""" Fuseki Client Implementation """

import logging
import os

import pylons
import requests
from SPARQLWrapper import SPARQLWrapper, POST
from ckanext.dcatde.triplestore.sparql_query_templates import DELETE_DATASET_BY_URI_SPARQL_QUERY

LOGGER = logging.getLogger(__name__)

UPDATE_ENDPOINT = 'update'
DATA_ENDPOINT = 'data'
PING_ENDPOINT = '$/ping'


class FusekiTriplestoreClient(object):
    """ A Client for communication with Fuseki-Triplestore Server """

    def __init__(self):
        self.fuseki_base_endpoint, self.fuseki_base_url = self._get_fuseki_urls()
        self.headers = {'Content-Type': 'application/rdf+xml'}

        if self.is_available():
            self.sparql_wrapper = SPARQLWrapper(self._get_update_endpoint())
            self.sparql_wrapper.setMethod(POST)

    def delete_dataset_in_triplestore(self, uri):
        """
        Delete a dataset in the triplestore
        :param uri: the uri of the dataset
        """
        LOGGER.debug(u'Deleting in triplestore: Dataset with URI: %s', uri)
        self.sparql_wrapper.setQuery(DELETE_DATASET_BY_URI_SPARQL_QUERY % {'uri': uri})
        results = self.sparql_wrapper.query()
        status_code = results.response.getcode()
        if status_code == 200:
            LOGGER.debug(u'Dataset in triple store successfully deleted')
        else:
            LOGGER.warn(u'Error! Deleting dataset URI %s response status != 200: %s', uri, str(status_code))

    def create_dataset_in_triplestore(self, graph, uri):
        """
        Create a new dataset in the triplestore
        :param graph: the dataset as rdf graph
        """
        LOGGER.debug(u'Creating new dataset in triplestore.')
        response = requests.post(self._get_data_endpoint(), data=graph, headers=self.headers)
        status_code = response.status_code
        if status_code == 200:
            LOGGER.debug(u'Dataset in triple store successfully created')
        else:
            LOGGER.warn(u'Error! Creating dataset URI %s response status != 200: %s', uri, str(status_code))

    def is_available(self):
        """
        Ping Fuseki Server to check availability
        :return True if successful
        """
        if self.fuseki_base_url is not None:
            response = requests.get(self._get_ping_endpoint())
            if response.status_code == 200:
                LOGGER.debug(u'Fuseki is available.')
                return True
            else:
                LOGGER.warn(u'Fuseki responded to ping with HTTP-Status %s! Skip updating data in ' \
                            u'Triplestore, because fuseki is not available!', str(response.status_code))
        return False

    def _get_update_endpoint(self):
        """ Returns the URL for the /update endpoint"""
        return os.path.join(self.fuseki_base_endpoint, '') + UPDATE_ENDPOINT

    def _get_data_endpoint(self):
        """ Returns the URL for the /data endpoint"""
        return os.path.join(self.fuseki_base_endpoint, '') + DATA_ENDPOINT

    def _get_ping_endpoint(self):
        """ Returns the URL for the $/ping endpoint"""
        return os.path.join(self.fuseki_base_url, '') + PING_ENDPOINT

    @staticmethod
    def _get_fuseki_urls():
        """ Read URLs for Fuseki from the config """
        fuseki_endpoint_base_url = None
        fuseki_base_url = None
        try:
            fuseki_base_url = pylons.config.get('ckanext.dcatde.fuseki.triplestore.url')
            datastore_name = pylons.config.get('ckanext.dcatde.fuseki.triplestore.name')
            fuseki_endpoint_base_url = os.path.join(fuseki_base_url, '') + datastore_name
        except (ValueError, AttributeError) as ex:
            LOGGER.info(u'Cannot read Fuseki URL from config: %s. TripleStore support is deactivated.',
                        ex.message)
        LOGGER.info(u'Found Fuseki URL in config. TripleStore support is activated.')
        return fuseki_endpoint_base_url, fuseki_base_url
