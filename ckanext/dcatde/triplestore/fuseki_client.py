#!/usr/bin/python
# -*- coding: utf8 -*-
""" Fuseki Client Implementation """

import logging
import os

import six
from ckan.plugins import toolkit as tk
import requests
from SPARQLWrapper import SPARQLWrapper, POST, JSON
from ckanext.dcatde.triplestore.sparql_query_templates import DELETE_DATASET_BY_URI_SPARQL_QUERY
from ckanext.dcatde.triplestore.sparql_query_templates import DELETE_DATASET_FROM_HARVEST_INFO_QUERY
from ckanext.dcatde.triplestore.sparql_query_templates import DELETE_VALIDATION_REPORT_BY_URI_SPARQL_QUERY

LOGGER = logging.getLogger(__name__)

UPDATE_ENDPOINT = 'update'
DATA_ENDPOINT = 'data'
QUERY_ENDPOINT = 'query'
PING_ENDPOINT = '$/ping'

CONTENT_TYPE_RDF_XML = 'application/rdf+xml'
CONTENT_TYPE_TURTLE = 'text/turtle'


class FusekiTriplestoreClient(object):
    """ A Client for communication with Fuseki-Triplestore Server """

    def __init__(self):
        self.fuseki_base_url, self.ds_name_default, self.ds_name_shacl_validation, self.ds_name_harvest_info = self._get_fuseki_config()

    def delete_dataset_in_triplestore(self, uri):
        """
        Delete a dataset in the triplestore
        :param uri: the uri of the dataset
        """
        self._delete_dataset_in_triplestore_base(
            uri, DELETE_DATASET_BY_URI_SPARQL_QUERY % {'uri': uri}, self.ds_name_default)

    def delete_dataset_in_triplestore_mqa(self, uri):
        """
        Delete a dataset in the triplestore
        :param uri: the uri of the dataset
        """
        self._delete_dataset_in_triplestore_base(
            uri, DELETE_VALIDATION_REPORT_BY_URI_SPARQL_QUERY % {'uri': uri},
            self.ds_name_shacl_validation)

    def delete_dataset_in_triplestore_harvest_info(self, uri):
        """
        Delete a dataset in the triplestore
        :param uri: the uri of the dataset
        """
        self._delete_dataset_in_triplestore_base(
            uri, DELETE_DATASET_FROM_HARVEST_INFO_QUERY % {'uri': uri},
            self.ds_name_harvest_info)

    def _delete_dataset_in_triplestore_base(self, uri, query_template, datastore_name):
        """
        Delete a dataset in the triplestore
        :param uri: the uri of the dataset
        """
        if not datastore_name:
            LOGGER.debug(u'No datastore name is given! Skipping...')
            return
        LOGGER.debug(u'Deleting in triplestore: Datastore name: %s, Dataset with URI %s', datastore_name, uri)
        result = self._query_sparql_wrapper(datastore_name, query_template)
        status_code = result.response.getcode()
        if status_code == 200:
            LOGGER.debug(u'Dataset in triple store successfully deleted')
        else:
            LOGGER.warning(u'Error! Deleting dataset URI %s response status != 200: %s', uri,
                           str(status_code))

    def create_dataset_in_triplestore(self, graph, uri):
        """
        Create a new dataset in the triplestore
        :param graph: the dataset as rdf graph
        """
        self._create_dataset_in_triplestore_base(graph, uri, self.ds_name_default, CONTENT_TYPE_TURTLE)

    def create_dataset_in_triplestore_mqa(self, graph, uri):
        """
        Create a new dataset in the triplestore
        :param graph: the dataset as rdf graph
        """
        self._create_dataset_in_triplestore_base(graph, uri, self.ds_name_shacl_validation,
                                                 CONTENT_TYPE_RDF_XML)

    def create_dataset_in_triplestore_harvest_info(self, graph, uri):
        """
        Create a new dataset in the triplestore
        :param graph: the dataset as rdf graph
        """
        self._create_dataset_in_triplestore_base(graph, uri, self.ds_name_harvest_info, CONTENT_TYPE_RDF_XML)

    def _create_dataset_in_triplestore_base(self, graph, uri, datastore_name, content_type):
        """
        Create a new dataset in the triplestore
        :param graph: the dataset as rdf graph
        """
        if not datastore_name:
            LOGGER.debug(u'No datastore name is given! Skipping...')
            return
        LOGGER.debug(u'Creating new dataset in triplestore. Datastore name: %s, Dataset with URI %s',
                     datastore_name, uri)
        if isinstance(graph, six.string_types):
            graph = graph.encode('utf-8')

        headers = {'Content-Type': content_type}
        response = requests.post(self._get_data_endpoint(datastore_name), data=graph, headers=headers)
        status_code = response.status_code
        if status_code == 200:
            LOGGER.debug(u'Dataset in triple store successfully created')
        else:
            LOGGER.warning(u'Error! Creating dataset URI %s response status != 200: %s', uri,
                           str(status_code))

    def select_datasets_in_triplestore_harvest_info(self, query):
        """
        Execute the query in the harvest_info datastore. Return the result.
        """
        return self._select_datasets_in_triplestore_base(query, self.ds_name_harvest_info)

    def _select_datasets_in_triplestore_base(self, query, datastore_name):
        """
        Create a new dataset in the triplestore
        :param query: query of the sparql request
        :param datastore_name: name of the dtastore
        """
        if not datastore_name:
            LOGGER.debug(u'No datastore name is given! Skipping...')
            return None
        sparql_wrapper = SPARQLWrapper(self._get_query_endpoint(datastore_name))
        sparql_wrapper.setQuery(query)
        sparql_wrapper.setMethod(POST)
        sparql_wrapper.setTimeout(10)
        sparql_wrapper.setReturnFormat(JSON)
        return sparql_wrapper.query()

    def is_available(self):
        """
        Ping Fuseki Server to check availability
        :return True if successful
        """
        if self.fuseki_base_url is not None:
            try:
                response = requests.get(self._get_ping_endpoint())
                if response.status_code == 200:
                    LOGGER.debug(u'Fuseki is available.')
                    return True
                else:
                    LOGGER.warning(u'Fuseki responded to ping with HTTP-Status %s! Skip updating data in ' \
                                   u'Triplestore, because fuseki is not available!',
                                   str(response.status_code))
            except requests.exceptions.RequestException as ex:
                LOGGER.warning(u'Exception occurred while connecting to Fuseki. Skip updating data in ' \
                               u'Triplestore, because fuseki is not available! Details: %s', ex)
        return False

    def _get_update_endpoint(self, datastore_name):
        """ Returns the URL for the /update endpoint"""
        return os.path.join(self.fuseki_base_url, datastore_name, '') + UPDATE_ENDPOINT

    def _get_data_endpoint(self, datastore_name):
        """ Returns the URL for the /data endpoint"""
        return os.path.join(self.fuseki_base_url, datastore_name, '') + DATA_ENDPOINT

    def _get_query_endpoint(self, datastore_name):
        """ Returns the URL for the /query endpoint"""
        return os.path.join(self.fuseki_base_url, datastore_name, '') + QUERY_ENDPOINT

    def _get_ping_endpoint(self):
        """ Returns the URL for the $/ping endpoint"""
        return os.path.join(self.fuseki_base_url, '') + PING_ENDPOINT

    @staticmethod
    def _get_fuseki_config():
        """ Read URLs for Fuseki from the config """
        fuseki_base_url = tk.config.get('ckanext.dcatde.fuseki.triplestore.url')
        datastore_name_default = tk.config.get('ckanext.dcatde.fuseki.triplestore.name')
        datastore_name_shacl_validation = tk.config.get('ckanext.dcatde.fuseki.shacl.store.name')
        datastore_name_harvest_info = tk.config.get('ckanext.dcatde.fuseki.harvest.info.name')
        if fuseki_base_url:
            LOGGER.info(u'Found Fuseki URL in config. TripleStore support is basically activated.')
            if not datastore_name_default:
                LOGGER.warning(u'Default datastore name NOT found! Saving datasets in the TripleStore is ' \
                            'DISABLED.')
            if not datastore_name_shacl_validation:
                LOGGER.warning(u'SHACL datastore name NOT found! Saving validation reports in the ' \
                               u'TripleStore is DISABLED.')
            if not datastore_name_harvest_info:
                LOGGER.warning(u'Harvest info datastore name NOT found! Deprecated datasets which are not ' \
                               u'stored in CKAN will not be deleted properly from the triplestore.')
        else:
            LOGGER.info(u'Cannot read Fuseki URL from config: %s. TripleStore support is deactivated.')
        return (fuseki_base_url, datastore_name_default, datastore_name_shacl_validation,
                datastore_name_harvest_info)

    def _query_sparql_wrapper(self, datastore_name, query):
        """ Queries the given query against the configured triplestore with the given datastore name. """
        sparql_wrapper = SPARQLWrapper(self._get_update_endpoint(datastore_name))
        sparql_wrapper.setQuery(query)
        sparql_wrapper.setMethod(POST)
        sparql_wrapper.setTimeout(10)
        return sparql_wrapper.query()
