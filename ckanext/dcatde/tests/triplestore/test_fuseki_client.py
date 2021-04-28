import unittest

from ckanext.dcatde.triplestore.fuseki_client import FusekiTriplestoreClient
from ckanext.dcatde.triplestore.sparql_query_templates import DELETE_DATASET_BY_URI_SPARQL_QUERY
from ckantoolkit.tests import helpers
from mock import patch
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, Namespace

FUSEKI_BASE_URL = 'http://foo:1010'
FUSEKI_BASE_DATASTORE_NAME = 'bar'
FUSEKI_ENDPOINT_URL = '{}/{}'.format(FUSEKI_BASE_URL, FUSEKI_BASE_DATASTORE_NAME)


class TestFusekiTriplestoreClient(unittest.TestCase):
    """
    Test class for the TestFusekiTriplestoreClient
    """
    DCAT = Namespace("http://www.w3.org/ns/dcat#")

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_fuseki_urls')
    def test_is_available_triplestore_not_reachable(self, mock_fuseki_get_urls):
        """ Tests if is_available() returns False if no triplestore is connected """

        mock_fuseki_get_urls.return_value = None, None
        client = FusekiTriplestoreClient()

        is_available_return = client.is_available()

        self.assertEquals(is_available_return, False)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_fuseki_urls')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.get')
    def test_is_available_triplestore_not_responding(self, mock_requests_get, mock_fuseki_get_urls):
        """ Tests if is_available() returns False if triplestore is connected but does not respond """

        mock_fuseki_get_urls.return_value = FUSEKI_ENDPOINT_URL, FUSEKI_BASE_URL
        mock_requests_get.return_value.status_code = 404

        client = FusekiTriplestoreClient()

        is_available_return = client.is_available()

        self.assertEquals(is_available_return, False)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_fuseki_urls')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.get')
    def test_is_available_triplestore_reachable(self, mock_requests_get, mock_fuseki_get_urls):
        """ Tests if is_available() returns True if triplestore is reachable """

        mock_fuseki_get_urls.return_value = FUSEKI_ENDPOINT_URL, FUSEKI_BASE_URL
        mock_requests_get.return_value.status_code = 200

        client = FusekiTriplestoreClient()

        is_available_return = client.is_available()

        self.assertEquals(is_available_return, True)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DATASTORE_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_load_config(self, mock_fuseki_available):
        """ Tests if config is read correctly """

        mock_fuseki_available.return_value = False

        client = FusekiTriplestoreClient()
        fuseki_endpoint_base_url, fuseki_base_url = client._get_fuseki_urls()

        self.assertEquals(fuseki_endpoint_base_url, '{}/{}'.format(FUSEKI_BASE_URL,
                                                                   FUSEKI_BASE_DATASTORE_NAME))
        self.assertEquals(fuseki_base_url, '{}'.format(FUSEKI_BASE_URL))

    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', None)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', None)
    def test_load_config_with_invalid_values(self):
        """ Tests if None is returned when config is not set properly """

        client = FusekiTriplestoreClient()
        fuseki_endpoint_base_url, fuseki_base_url = client._get_fuseki_urls()

        self.assertIsNone(fuseki_endpoint_base_url)
        self.assertIsNone(fuseki_base_url)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.name', FUSEKI_BASE_DATASTORE_NAME)
    @helpers.change_config('ckanext.dcatde.fuseki.triplestore.url', FUSEKI_BASE_URL)
    def test_get_update_endpoint_url(self, mock_fuseki_available):
        """ Tests if endpoint URLs are built properly """

        mock_fuseki_available.return_value = False

        client = FusekiTriplestoreClient()

        self.assertEquals(client._get_update_endpoint(), '{}/update'.format(FUSEKI_ENDPOINT_URL))
        self.assertEquals(client._get_data_endpoint(), '{}/data'.format(FUSEKI_ENDPOINT_URL))
        self.assertEquals(client._get_ping_endpoint(), '{}/$/ping'.format(FUSEKI_BASE_URL))

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_fuseki_urls')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.post')
    def test_create_dataset_successful(self, mock_requests_post, mock_fuseki_get_urls,
                                       mock_fuseki_is_available):
        """ Tests create is called with correct parameters """

        uri = "http://example.org/datasets/1"
        g = Graph()
        g.add((URIRef(uri), RDF.type, self.DCAT.Dataset))

        mock_requests_post.return_value.status_code = 200
        mock_fuseki_get_urls.return_value = FUSEKI_ENDPOINT_URL, FUSEKI_BASE_URL
        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.create_dataset_in_triplestore(g, uri)

        mock_requests_post.assert_called_once_with('{}/data'.format(FUSEKI_ENDPOINT_URL),
                                                   data=g, headers=client.headers)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_fuseki_urls')
    @patch('ckanext.dcatde.triplestore.fuseki_client.requests.post')
    def test_create_dataset_unsuccessful(self, mock_requests_post, mock_fuseki_get_urls,
                                         mock_fuseki_is_available):
        """ Tests create is called with correct parameters """

        uri = "http://example.org/datasets/1"
        g = Graph()
        g.add((URIRef(uri), RDF.type, self.DCAT.Dataset))

        mock_requests_post.return_value.status_code = 404
        mock_fuseki_get_urls.return_value = FUSEKI_ENDPOINT_URL, FUSEKI_BASE_URL
        mock_fuseki_is_available.return_value = True

        client = FusekiTriplestoreClient()
        client.create_dataset_in_triplestore(g, uri)

        mock_requests_post.assert_called_once_with('{}/data'.format(FUSEKI_ENDPOINT_URL),
                                                   data=g, headers=client.headers)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_update_endpoint')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.setQuery')
    @patch('ckanext.dcatde.triplestore.fuseki_client.SPARQLWrapper.query')
    def test_delete_dataset(self, mock_sparql_query, mock_sparql_set_query, mock_fuseki_update_endpoint,
                            mock_fuseki_is_avilable):
        """ Tests query for deletion is set properly """

        test_uri = URIRef("http://example.org/datasets/1")

        mock_fuseki_is_avilable.return_value = True
        mock_fuseki_update_endpoint.return_value = '{}/update'.format(FUSEKI_ENDPOINT_URL)

        client = FusekiTriplestoreClient()
        client.delete_dataset_in_triplestore(test_uri)

        # set_query() is called in SPARQLWrapper-init() as well, so we can't check for called_once
        self.assertEquals(mock_sparql_set_query.call_count, 2)
        mock_sparql_set_query.assert_called_with(DELETE_DATASET_BY_URI_SPARQL_QUERY % {'uri': str(test_uri)})
        mock_sparql_query.assert_called_once_with()
