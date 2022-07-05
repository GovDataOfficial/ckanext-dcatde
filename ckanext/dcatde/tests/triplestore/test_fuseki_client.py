import unittest

import requests
from ckanext.dcatde.triplestore.fuseki_client import (
    FusekiTriplestoreClient, CONTENT_TYPE_RDF_XML, CONTENT_TYPE_TURTLE)
from ckanext.dcatde.triplestore.sparql_query_templates import DELETE_DATASET_BY_URI_SPARQL_QUERY
from ckanext.dcatde.triplestore.sparql_query_templates import DELETE_DATASET_FROM_HARVEST_INFO_QUERY
from ckanext.dcatde.triplestore.sparql_query_templates import DELETE_VALIDATION_REPORT_BY_URI_SPARQL_QUERY
from ckanext.dcatde.triplestore.sparql_query_templates import GET_URIS_FROM_HARVEST_INFO_QUERY
from ckantoolkit.tests import helpers
from mock import patch
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, Namespace

FUSEKI_BASE_URL = 'http://foo:1010'
FUSEKI_BASE_DS_NAME = 'bar'
FUSEKI_SHACL_DS_NAME = 'foo'
FUSEKI_HARVEST_DS_NAME = 'foobar'
FUSEKI_ENDPOINT_URL = '{}/{}'.format(FUSEKI_BASE_URL, FUSEKI_BASE_DS_NAME)
FUSEKI_SHACL_ENDPOINT_URL = '{}/{}'.format(FUSEKI_BASE_URL, FUSEKI_SHACL_DS_NAME)
FUSEKI_HARVEST_ENDPOINT_URL = '{}/{}'.format(FUSEKI_BASE_URL, FUSEKI_HARVEST_DS_NAME)
HEADERS_CONTENT_TYPE_TURTLE = {'Content-Type': CONTENT_TYPE_TURTLE}
HEADERS_CONTENT_TYPE_RDF_XML = {'Content-Type': CONTENT_TYPE_RDF_XML}


class TestFusekiTriplestoreClient(unittest.TestCase):
    """
    Test class for the TestFusekiTriplestoreClient
    """
    DCAT = Namespace("http://www.w3.org/ns/dcat#")

    @helpers.change_config('ckanext.dcatde.fuseki.harvester.info.name', None)
    @helpers.change_config('ckanext.dcatde.fuseki.shacl.store.name', None)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', None)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', None)
    def test_is_available_triplestore_not_reachable(self):
        """ Tests if is_available() returns False if no triplestore is connected """

        client = FusekiTriplestoreClient()

        is_available_return = client.is_available()

        self.assertEqual(is_available_return, False)

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.get')
    def test_is_available_triplestore_not_responding(self, mock_requests_get):
        """ Tests if is_available() returns False if triplestore is connected but does not respond """

        mock_requests_get.return_value.status_code = 404

        client = FusekiTriplestoreClient()

        is_available_return = client.is_available()

        self.assertEqual(is_available_return, False)

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.get')
    def test_is_available_triplestore_reachable(self, mock_requests_get):
        """ Tests if is_available() returns True if triplestore is reachable """

        mock_requests_get.return_value.status_code = 200

        client = FusekiTriplestoreClient()

        is_available_return = client.is_available()

        self.assertEqual(is_available_return, True)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.shacl.store.name', FUSEKI_SHACL_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_load_config(self, mock_fuseki_available):
        """ Tests if config is read correctly """

        mock_fuseki_available.return_value = False

        client = FusekiTriplestoreClient()
        fuseki_base_url, ds_name_default, ds_name_shacl, ds_name_harvest = client._get_fuseki_config()

        self.assertEqual(ds_name_harvest, FUSEKI_HARVEST_DS_NAME)
        self.assertEqual(ds_name_shacl, FUSEKI_SHACL_DS_NAME)
        self.assertEqual(ds_name_default, FUSEKI_BASE_DS_NAME)
        self.assertEqual(fuseki_base_url, FUSEKI_BASE_URL)

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.get')
    def test_is_available_triplestore_request_exception(self, mock_requests_get):
        """ Tests if is_available() returns False if raises an exception while connecting triplestore. """

        mock_requests_get.return_value.status_code = requests.exceptions.ConnectionError('test_error')

        client = FusekiTriplestoreClient()

        is_available_return = client.is_available()

        self.assertEqual(is_available_return, False)

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', None)
    @helpers.change_config('ckanext.dcatde.fuseki.shacl.store.name', None)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', None)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', None)
    def test_load_config_with_invalid_values(self):
        """ Tests if None is returned when config is not set properly """

        client = FusekiTriplestoreClient()
        fuseki_base_url, ds_name_default, ds_name_shacl, ds_name_harvest = client._get_fuseki_config()

        self.assertIsNone(ds_name_harvest)
        self.assertIsNone(ds_name_shacl)
        self.assertIsNone(ds_name_default)
        self.assertIsNone(fuseki_base_url)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_get_endpoint_urls(self, mock_fuseki_available):
        """ Tests if endpoint URLs are built properly """

        mock_fuseki_available.return_value = False

        client = FusekiTriplestoreClient()

        self.assertEqual(
            client._get_update_endpoint(FUSEKI_BASE_DS_NAME), '{}/update'.format(FUSEKI_ENDPOINT_URL))
        self.assertEqual(
            client._get_data_endpoint(FUSEKI_BASE_DS_NAME), '{}/data'.format(FUSEKI_ENDPOINT_URL))
        self.assertEqual(
            client._get_query_endpoint(FUSEKI_BASE_DS_NAME), '{}/query'.format(FUSEKI_ENDPOINT_URL))
        self.assertEqual(
            client._get_update_endpoint(FUSEKI_SHACL_DS_NAME), '{}/update'.format(FUSEKI_SHACL_ENDPOINT_URL))
        self.assertEqual(
            client._get_data_endpoint(FUSEKI_SHACL_DS_NAME), '{}/data'.format(FUSEKI_SHACL_ENDPOINT_URL))
        self.assertEqual(
            client._get_query_endpoint(FUSEKI_SHACL_DS_NAME), '{}/query'.format(FUSEKI_SHACL_ENDPOINT_URL))
        self.assertEqual(client._get_ping_endpoint(), '{}/$/ping'.format(FUSEKI_BASE_URL))

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.post')
    def test_create_dataset_successful(self, mock_requests_post, mock_fuseki_is_available):
        """ Tests create is called with correct parameters """

        uri = "http://example.org/datasets/1"
        g = Graph()
        g.add((URIRef(uri), RDF.type, self.DCAT.Dataset))

        mock_requests_post.return_value.status_code = 200
        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.create_dataset_in_triplestore(g, uri)

        mock_requests_post.assert_called_once_with('{}/data'.format(FUSEKI_ENDPOINT_URL),
                                                   data=g, headers=HEADERS_CONTENT_TYPE_TURTLE)

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.shacl.store.name', FUSEKI_SHACL_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.post')
    def test_create_dataset_successful_mqa(self, mock_requests_post, mock_fuseki_is_available):
        """ Tests create MQA is called with correct parameters """

        uri = "http://example.org/datasets/1"
        g = Graph()
        g.add((URIRef(uri), RDF.type, self.DCAT.Dataset))

        mock_requests_post.return_value.status_code = 200
        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.create_dataset_in_triplestore_mqa(g, uri)

        mock_requests_post.assert_called_once_with('{}/data'.format(FUSEKI_SHACL_ENDPOINT_URL),
                                                   data=g, headers=HEADERS_CONTENT_TYPE_RDF_XML)

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.post')
    def test_create_dataset_successful_harvest_info(self, mock_requests_post, mock_fuseki_is_available):
        """ Tests create MQA is called with correct parameters """

        uri = "http://example.org/datasets/1"
        g = Graph()
        g.add((URIRef(uri), RDF.type, self.DCAT.Dataset))

        mock_requests_post.return_value.status_code = 200
        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.create_dataset_in_triplestore_harvest_info(g, uri)

        mock_requests_post.assert_called_once_with('{}/data'.format(FUSEKI_HARVEST_ENDPOINT_URL),
                                                   data=g, headers=HEADERS_CONTENT_TYPE_RDF_XML)

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.post')
    def test_create_dataset_unsuccessful_404(self, mock_requests_post, mock_fuseki_is_available):
        """ Tests create gets 404 from server """

        uri = "http://example.org/datasets/1"
        g = Graph()
        g.add((URIRef(uri), RDF.type, self.DCAT.Dataset))

        mock_requests_post.return_value.status_code = 404
        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.create_dataset_in_triplestore(g, uri)

        mock_requests_post.assert_called_once_with('{}/data'.format(FUSEKI_ENDPOINT_URL),
                                                   data=g, headers=HEADERS_CONTENT_TYPE_TURTLE)

    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.post')
    def test_create_dataset_base_ds_name_none(self, mock_requests_post, mock_fuseki_is_available):
        """ Tests create gets 404 from server """

        uri = "http://example.org/datasets/1"
        g = Graph()
        g.add((URIRef(uri), RDF.type, self.DCAT.Dataset))

        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client._create_dataset_in_triplestore_base(g, uri, None, CONTENT_TYPE_RDF_XML)

        mock_requests_post.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.setQuery')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.query')
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_delete_dataset(self, mock_sparql_query, mock_sparql_set_query, mock_fuseki_is_available):
        """ Tests query for deletion is set properly """

        test_uri = URIRef("http://example.org/datasets/1")

        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.delete_dataset_in_triplestore(test_uri)

        # set_query() is called in SPARQLWrapper-init() as well, so we can't check for called_once
        self.assertEqual(mock_sparql_set_query.call_count, 2)
        mock_sparql_set_query.assert_called_with(DELETE_DATASET_BY_URI_SPARQL_QUERY % {'uri': str(test_uri)})
        mock_sparql_query.assert_called_once_with()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.setQuery')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.query')
    @helpers.change_config('ckanext.dcatde.fuseki.shacl.store.name', FUSEKI_SHACL_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_delete_dataset_mqa(self, mock_sparql_query, mock_sparql_set_query, mock_fuseki_is_available):
        """ Tests query for MQA deletion is set properly """

        test_uri = URIRef("http://example.org/datasets/1")

        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.delete_dataset_in_triplestore_mqa(test_uri)

        # set_query() is called in SPARQLWrapper-init() as well, so we can't check for called_once
        self.assertEqual(mock_sparql_set_query.call_count, 2)
        mock_sparql_set_query.assert_called_with(
            DELETE_VALIDATION_REPORT_BY_URI_SPARQL_QUERY % {'uri': str(test_uri)})
        mock_sparql_query.assert_called_once_with()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.setQuery')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.query')
    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_delete_dataset_harvest_info(self, mock_sparql_query, mock_sparql_set_query, mock_fuseki_is_available):
        """ Tests query for harvest_info deletion is set properly """

        test_uri = URIRef("http://example.org/datasets/1")

        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.delete_dataset_in_triplestore_harvest_info(test_uri)

        # set_query() is called in SPARQLWrapper-init() as well, so we can't check for called_once
        self.assertEqual(mock_sparql_set_query.call_count, 2)
        mock_sparql_set_query.assert_called_with(
            DELETE_DATASET_FROM_HARVEST_INFO_QUERY % {'uri': str(test_uri)})
        mock_sparql_query.assert_called_once_with()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.query')
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_delete_dataset_base_ds_name_none(self, mock_sparql_query, mock_fuseki_is_available):
        """ Tests query for deletion is set properly """

        test_uri = URIRef("http://example.org/datasets/1")

        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client._delete_dataset_in_triplestore_base(test_uri, DELETE_DATASET_BY_URI_SPARQL_QUERY, None)

        mock_sparql_query.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.setQuery')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.query')
    @helpers.change_config('ckanext.dcatde.fuseki.harvest.info.name', FUSEKI_HARVEST_DS_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_select_datasets_in_triplestore_harvest_info(self, mock_sparql_query, mock_sparql_set_query,
                                                         mock_fuseki_is_available):
        """ Test if query for selection is set properly and if results returned """
        owner_org = 'org-1'
        mock_response = {"foo": "bar"}
        test_query = GET_URIS_FROM_HARVEST_INFO_QUERY % {'owner_org_or_source_id': owner_org}

        mock_fuseki_is_available.return_value = True
        mock_sparql_query.return_value = mock_response

        client = FusekiTriplestoreClient()
        result = client.select_datasets_in_triplestore_harvest_info(test_query)
        self.assertEqual(result, mock_response)
        mock_sparql_query.assert_called_once_with()
        mock_sparql_set_query.assert_called_with(test_query)
