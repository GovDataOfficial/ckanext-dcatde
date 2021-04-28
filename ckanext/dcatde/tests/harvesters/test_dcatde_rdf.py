#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import unittest

import pkg_resources
from ckanext.dcat.processors import RDFParser
from ckanext.dcatde.harvesters.dcatde_rdf import DCATdeRDFHarvester
from ckantoolkit.tests import helpers
from mock import patch, Mock, ANY
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, Namespace


class TestDCATdeRDFHarvester(unittest.TestCase):
    """
    Test class for the DCATdeRDFHarvester
    """
    DCAT = Namespace("http://www.w3.org/ns/dcat#")

    @staticmethod
    def _get_harvest_obj_dummy(portal, status):
        """
        Builds a mocked Harvest object.

        :param portal: The harvested_portal config setting
        :param status: The harvest object status string
        :return: Mocked harvest object
        """
        obj_content = json.dumps({
            'id': 'test-id',
            'name': 'test-name'
        })
        source_config = json.dumps({
            'harvested_portal': portal
        })

        harvest_src = Mock(config=source_config)
        harvest_extra = Mock(key='status', value=status)
        harvest_obj = Mock(content=obj_content, package_id='test-id', guid='guid-123',
                           source=harvest_src, extras=[harvest_extra])

        return harvest_obj

    @staticmethod
    def _prepare_obj_with_resources(resources):
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')
        obj_content = json.loads(harvest_obj.content)
        obj_content['resources'] = resources
        harvest_obj.content = json.dumps(obj_content)
        return harvest_obj

    @staticmethod
    def _prepare_license_fallback_obj():
        return TestDCATdeRDFHarvester._prepare_obj_with_resources([
            {'uri': 'http://example.com/no-license'},
            {'uri': 'http://example.com/license', 'license': 'foo'},
        ])

    @staticmethod
    def _pepare_license_migration_obj(license1, license2):
        return TestDCATdeRDFHarvester._prepare_obj_with_resources([
            {'uri': 'http://example.com/1', 'license': license1},
            {'uri': 'http://example.com/2', 'license': license2}
        ])

    def _assert_resource_licenses(self, harvest_obj, expected_first, expected_second):
        updated_content = json.loads(harvest_obj.content)
        updated_resources = updated_content.get('resources')
        self.assertEquals(updated_resources[0]['license'], expected_first)
        self.assertEquals(updated_resources[1]['license'], expected_second)

    @staticmethod
    def _get_max_rdf(self, item_name="metadata_max"):
        data = pkg_resources.resource_string(__name__,
                                             "../resources/%s.rdf" % item_name)
        return data

    @staticmethod
    def _get_uris_from_rdf(rdf_parser):
        return rdf_parser._datasets()

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_metadata_on_import(self, mock_super_import):
        """
        Tests if metadata_harvested_portal is set for a package in the import_stage.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'no-delete')

        # run
        harvester.import_stage(harvest_obj)

        updated_content = json.loads(harvest_obj.content)

        # check if import of the base class was called
        mock_super_import.assert_called_with(harvest_obj)

        # check if the extras field is set properly
        for extra in updated_content.get('extras'):
            if extra['key'] == 'metadata_harvested_portal':
                self.assertEquals(extra['value'], 'testportal')
                return

        self.fail("extras.metadata_harvested_portal was not set")

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_license_fallback_without_cfg(self, mock_super_import):
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._prepare_license_fallback_obj()

        # run
        harvester.import_stage(harvest_obj)

        # check
        self._assert_resource_licenses(harvest_obj, u'http://dcat-ap.de/def/licenses/other-closed', u'foo')

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    @helpers.change_config('ckanext.dcatde.harvest.default_license', 'test-license')
    def test_license_fallback_with_cfg(self, mock_super_import):
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._prepare_license_fallback_obj()

        # run
        harvester.import_stage(harvest_obj)

        # check
        self._assert_resource_licenses(harvest_obj, u'test-license', u'foo')

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.load_json_mapping')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    @helpers.change_config('ckanext.dcatde.urls.dcat_licenses_upgrade_mapping', 'test-file')
    def test_license_migration(self, mock_super_import, mock_load_mapping):
        # prepare
        mock_load_mapping.return_value = {
            'foo': 'foo_new',
            'bar': 'bar'
        }
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._pepare_license_migration_obj('foo', 'bar')

        # run
        harvester.import_stage(harvest_obj)

        # check
        self._assert_resource_licenses(harvest_obj, u'foo_new', u'bar')

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.load_json_mapping')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    @helpers.change_config('ckanext.dcatde.urls.dcat_licenses_upgrade_mapping', 'test-file')
    def test_license_migration_unknown(self, mock_super_import, mock_load_mapping):
        # unknown licenses should remain unchanged
        # prepare
        mock_load_mapping.return_value = {
            'foo': 'foo_new',
            'bar': 'bar'
        }
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._pepare_license_migration_obj('foo', 'other')

        # run
        harvester.import_stage(harvest_obj)

        # check
        self._assert_resource_licenses(harvest_obj, u'foo_new', u'other')

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.load_json_mapping')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    @helpers.change_config('ckanext.dcatde.urls.dcat_licenses_upgrade_mapping', 'test-file')
    def test_license_migration_empty_mapping(self, mock_super_import, mock_load_mapping):
        # Nothing should be changed if mapping is empty
        # prepare
        mock_load_mapping.return_value = {}
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._pepare_license_migration_obj('foo', 'other')

        # run
        harvester.import_stage(harvest_obj)

        # check
        self._assert_resource_licenses(harvest_obj, u'foo', u'other')

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.load_json_mapping')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_license_migration_no_config(self, mock_super_import, mock_load_mapping):
        # If the config parameter isn't set, also nothing should be changed
        # prepare
        mock_load_mapping.return_value = {
            'foo': 'foo_new'
        }
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._pepare_license_migration_obj('foo', 'other')

        # run
        harvester.import_stage(harvest_obj)

        # check
        mock_load_mapping.assert_not_called()
        self._assert_resource_licenses(harvest_obj, u'foo', u'other')

    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.rename_delete_dataset_with_id')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_custom_delete(self, mock_super_import, mock_deletion):
        """
        Tests if the dataset deletion logic is independent of the base implementation and that
        the custom renaming logic from HarvestUtils is used.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'delete')

        # run
        result = harvester.import_stage(harvest_obj)

        # check
        self.assertTrue(result)
        # no call to the base logic was made
        mock_super_import.assert_not_called()
        mock_deletion.assert_called_with('test-id')

    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.handle_duplicates')
    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.rename_delete_dataset_with_id')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_handle_duplicates_accept(self, mock_super_import, mock_deletion, mock_handle_duplicates):
        """
        Tests if the dataset duplicate detection logic is called and the super implementation of import_stage
        is called, if the import is accepted.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', '')
        # Import accepted
        mock_handle_duplicates.return_value = True

        # run
        result = harvester.import_stage(harvest_obj)

        # check
        self.assertTrue(result)
        # no call to the custom delete logic was made
        mock_deletion.assert_not_called()
        mock_handle_duplicates.assert_called_with(harvest_obj.content)
        mock_super_import.assert_called_with(harvest_obj)

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.handle_duplicates')
    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.rename_delete_dataset_with_id')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_handle_duplicates_skip(self, mock_super_import, mock_deletion, mock_handle_duplicates,
                                           mock_save_object_error):
        """
        Tests if the dataset duplicate detection logic is called and the super implementation of import_stage
        is NOT called, if the import is skipped.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', '')
        dataset_dict = json.loads(harvest_obj.content)
        # Import skipped
        mock_handle_duplicates.return_value = False

        # run
        result = harvester.import_stage(harvest_obj)

        # check
        self.assertFalse(result)
        # no call to the custom delete logic was made
        mock_deletion.assert_not_called()
        mock_handle_duplicates.assert_called_with(harvest_obj.content)
        mock_save_object_error.assert_called_with(
            'Skipping importing dataset, because of duplicate detection!', harvest_obj, 'Import')
        mock_super_import.assert_not_called()

    def _assert_no_resources_error(self, harvest_obj, mock_save_object_error):
        # check (do not require exact string message but look for keywords)
        mock_save_object_error.assert_called_once_with(
            ANY, harvest_obj, 'Import'
        )
        message = mock_save_object_error.call_args[0][0].lower()
        self.assertTrue('skip' in message)
        self.assertTrue('no resources' in message)

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._read_datasets_from_db')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_no_resources_skip(self, mock_super_import, mock_save_object_error,
                                      mock_read_datasets_from_db):
        """
        Tests if datasets without resources are skipped if configured.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', '')
        # add resources required config
        cfg = json.loads(harvest_obj.source.config)
        cfg['resources_required'] = True
        harvest_obj.source.config = json.dumps(cfg)
        mock_read_datasets_from_db.return_value = []

        # run
        harvester.import_stage(harvest_obj)

        # check _read_datasets_from_db is called with the guid of the harvest object
        mock_read_datasets_from_db.assert_called_once_with('guid-123')
        # check if error was saved
        self._assert_no_resources_error(harvest_obj, mock_save_object_error)
        # dataset should be skipped
        mock_super_import.assert_not_called()

    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.rename_delete_dataset_with_id')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._read_datasets_from_db')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_no_resources_skip_dataset_exists(self, mock_super_import, mock_save_object_error,
                                                     mock_read_datasets_from_db, mock_deletion):
        """
        Tests if datasets without resources are skipped if configured.
        When a local dataset exists, it should be removed in addition.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', '')
        # add resources required config
        cfg = json.loads(harvest_obj.source.config)
        cfg['resources_required'] = True
        harvest_obj.source.config = json.dumps(cfg)
        mock_read_datasets_from_db.return_value = [['test-id', 'extra']]

        # run
        harvester.import_stage(harvest_obj)

        # check _read_datasets_from_db is called with the guid of the harvest object
        mock_read_datasets_from_db.assert_called_once_with('guid-123')
        # check if error was saved
        self._assert_no_resources_error(harvest_obj, mock_save_object_error)
        # local dataset should be deleted
        mock_deletion.assert_called_once_with('test-id')
        # dataset should not be imported
        mock_super_import.assert_not_called()

    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.rename_delete_dataset_with_id')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._read_datasets_from_db')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_no_resources_skip_dataset_exists_multiple(self, mock_super_import, mock_save_object_error,
                                                              mock_read_datasets_from_db, mock_deletion):
        """
        Tests if datasets without resources are skipped if configured.
        If multiple local datasets with the same GUID exist, nothing should be deleted.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', '')
        # add resources required config
        cfg = json.loads(harvest_obj.source.config)
        cfg['resources_required'] = True
        harvest_obj.source.config = json.dumps(cfg)
        mock_read_datasets_from_db.return_value = [['test-id', 'extra'], ['other-id', 'extra']]

        # run
        harvester.import_stage(harvest_obj)

        # check _read_datasets_from_db is called with the guid of the harvest object
        mock_read_datasets_from_db.assert_called_once_with('guid-123')
        # check if error was saved
        self._assert_no_resources_error(harvest_obj, mock_save_object_error)
        # nothing should be deleted
        mock_deletion.assert_not_called()
        # dataset should be skipped nevertheless
        mock_super_import.assert_not_called()

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._read_datasets_from_db')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_no_resources_empty_list_skip(self, mock_super_import, mock_save_object_error,
                                                 mock_read_datasets_from_db):
        """
        Tests if datasets without resources are skipped if configured.
        Empty resource lists should be skipped as well.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', '')
        # add resources required config
        cfg = json.loads(harvest_obj.source.config)
        cfg['resources_required'] = True
        harvest_obj.source.config = json.dumps(cfg)
        harvest_object_content = json.loads(harvest_obj.content)
        harvest_object_content['resources'] = []
        harvest_obj.content = json.dumps(harvest_object_content)
        mock_read_datasets_from_db.return_value = []

        # run
        harvester.import_stage(harvest_obj)

        # check _read_datasets_from_db is called with the guid of the harvest object
        mock_read_datasets_from_db.assert_called_once_with('guid-123')
        # check if error was saved
        self._assert_no_resources_error(harvest_obj, mock_save_object_error)
        # dataset should be skipped
        mock_super_import.assert_not_called()

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._read_datasets_from_db')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_no_resources_default_behavior(self, mock_super_import, mock_save_object_error,
                                                  mock_read_datasets_from_db):
        """
        Tests if datasets without resources are imported when the skip option is not set.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', '')
        # do not configure resources_required parameter

        # run
        harvester.import_stage(harvest_obj)

        # check not calling _read_datasets_from_db if config option resources_required is not set or False
        mock_read_datasets_from_db.assert_not_called()
        # check (dataset should be imported anyway, without error)
        mock_save_object_error.assert_not_called()

        mock_super_import.assert_called_once_with(harvest_obj)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_update_endpoint')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_data_endpoint')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    def test_harvesting_one_dataset_after_parse(self, mock_fuseki_create_data, mock_fuseki_delete_data,
                                                mock_fuseki_data_endpoint, mock_fuseki_update_endpoint,
                                                mock_fuseki_available):
        """
        Test valid content in after_parsing() and check if correct methods are being called.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        maxrdf = self._get_max_rdf('metadata_max')
        rdf_parser = RDFParser()
        rdf_parser.parse(maxrdf, 'application/rdf+xml')

        mock_fuseki_available.return_value = True
        mock_fuseki_update_endpoint.return_value = 'http://localhost:3030/ds/update'
        mock_fuseki_data_endpoint.return_value = 'http://localhost:3030/ds/data'

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, None)

        # the parser should not have changed
        self.assertEquals(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEquals(len(error_msgs), 0)
        # check if delete dataset was called
        mock_fuseki_delete_data.assert_called_once_with(rdf_parser._datasets().next())  # Testdata has only one dataset
        # check if create dataset was called
        mock_fuseki_create_data.assert_called_once_with(ANY)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    def test_harvesting_after_parse_triplestore_not_available(self, mock_fuseki_create_data, mock_fuseki_delete_data):
        """
        Test behaviour if triplestore is not available
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        rdf_parser = RDFParser()

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, None)

        # the parser should not have changed
        self.assertEquals(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEquals(len(error_msgs), 0)
        # create dataset should not be called
        mock_fuseki_create_data.assert_not_called()
        # delete dataset should not be called
        mock_fuseki_delete_data.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    def test_harvesting_after_parse_rdf_parser_not_available(self, mock_fuseki_create_data, mock_fuseki_delete_data):
        """
        Test behaviour if RDF-Parser is not available
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        rdf_parser = None

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, None)

        # the parser should not have changed
        self.assertEquals(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEquals(len(error_msgs), 0)
        # create dataset should not be called
        mock_fuseki_create_data.assert_not_called()
        # delete dataset should not be called
        mock_fuseki_delete_data.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_update_endpoint')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient._get_data_endpoint')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    def test_harvesting_multiple_datasets_after_parse(self, mock_fuseki_create_data, mock_fuseki_delete_data,
                                                      mock_fuseki_data_endpoint, mock_fuseki_update_endpoint,
                                                      mock_fuseki_available):
        """
        Test valid content in after_parsing() and check if correct methods are being called.
        """
        # prepare
        uris = [URIRef("http://example.org/datasets/1"), URIRef("http://example.org/datasets/2")]
        g = Graph()
        for uri in uris:
            g.add((uri, RDF.type, self.DCAT.Dataset))

        rdf_parser = RDFParser()
        rdf_parser.g = g
        harvester = DCATdeRDFHarvester()

        mock_fuseki_available.return_value = True
        mock_fuseki_update_endpoint.return_value = 'http://localhost:3030/ds/update'
        mock_fuseki_data_endpoint.return_value = 'http://localhost:3030/ds/data'

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, None)

        # the parser should not have changed
        self.assertEquals(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEquals(len(error_msgs), 0)
        # check if delete dataset was called twice
        self.assertEquals(mock_fuseki_delete_data.call_count, len(uris))
        # check if create dataset was called twice
        self.assertEquals(mock_fuseki_create_data.call_count, len(uris))
