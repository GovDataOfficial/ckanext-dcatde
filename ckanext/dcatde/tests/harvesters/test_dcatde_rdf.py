#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import unittest

from parameterized import parameterized
import pkg_resources
from SPARQLWrapper.SPARQLExceptions import SPARQLWrapperException
from SPARQLWrapper.Wrapper import QueryResult
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, Namespace, FOAF
from ckanext.dcat.processors import RDFParser
from ckanext.dcatde.dataset_utils import EXTRA_KEY_HARVESTED_PORTAL
from ckanext.dcatde.harvesters.dcatde_rdf import DCATdeRDFHarvester
from ckanext.dcatde.triplestore.sparql_query_templates import GET_URIS_FROM_HARVEST_INFO_QUERY
from ckantoolkit.tests import helpers
from mock import call, patch, Mock, ANY, DEFAULT


class TestDCATdeRDFHarvester(unittest.TestCase):
    """
    Test class for the DCATdeRDFHarvester
    """
    DCAT = Namespace("http://www.w3.org/ns/dcat#")

    @staticmethod
    def _get_harvest_obj_dummy(portal, status):
        """
        Builds a mocked Harvest object.

        :param portal: The harvested_portal config setting (optional)
        :param status: The harvest object status string
        :return: Mocked harvest object
        """
        obj_content = json.dumps({
            'id': 'test-id',
            'name': 'test-name'
        })

        config_dict = {
            'contributorID': 'http://dcat-ap.de/def/contributors/testId'
        }
        if portal is not None:
            config_dict['harvested_portal'] = portal

        source_config = json.dumps(config_dict)

        harvest_src = Mock(config=source_config, id='test-id-123')
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
        self.assertEqual(updated_resources[0]['license'], expected_first)
        self.assertEqual(updated_resources[1]['license'], expected_second)

    @staticmethod
    def _get_max_rdf(item_name="metadata_max"):
        data = pkg_resources.resource_string(__name__,
                                             "../resources/%s.rdf" % item_name)
        return data

    @staticmethod
    def _get_rdf(uri):
        rdf = '''<?xml version="1.0" encoding="utf-8"?>
        <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:dcat="http://www.w3.org/ns/dcat#">
        <dcat:Dataset 
            rdf:about="%s" />
        </rdf:RDF>''' % uri
        return rdf

    def _assert_no_resources_error(self, harvest_obj, mock_save_object_error):
        # check (do not require exact string message but look for keywords)
        mock_save_object_error.assert_called_once_with(
            ANY, harvest_obj, 'Import'
        )
        message = mock_save_object_error.call_args[0][0].lower()
        self.assertTrue('skip' in message)
        self.assertTrue('no resources' in message)

    def _assert_rdf_harvest_info(self, mock_call_args_list, uris, owner_org, harvest_source_id):
        assert len(uris) > 0
        assert len(mock_call_args_list) == len(uris)
        for calls in mock_call_args_list:
            args, kwargs = calls
            graph_from_mock_args = Graph().parse(data=args[0], format='application/rdf+xml')
            object_list = []
            for s, p, o in graph_from_mock_args.triples((None, None, None)):
                self.assertIn(s, [URIRef(uri) for uri in uris])
                self.assertEqual(p, FOAF.knows)
                object_list.append(o)
            self.assertCountEqual(object_list, [Literal(owner_org), Literal(harvest_source_id)])

    @parameterized.expand([
        'testportal',
        None
    ])
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_metadata_on_import(self, harvested_portal, mock_super_import):
        """
        Tests if metadata_harvested_portal is set for a package in the import_stage when configured.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy(harvested_portal, 'no-delete')

        # run
        harvester.import_stage(harvest_obj)

        updated_content = json.loads(harvest_obj.content)

        # check if import of the base class was called
        mock_super_import.assert_called_with(harvest_obj)

        # check if the extras field is set properly if metadata_harvested_portal is configured
        if harvested_portal:
            self.assertIn({'key': EXTRA_KEY_HARVESTED_PORTAL, 'value': 'testportal'},
                          updated_content.get('extras', []))
        else:
            self.assertNotIn(EXTRA_KEY_HARVESTED_PORTAL,
                             [extra['key'] for extra in updated_content.get('extras', [])])

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
    def test_add_contributor_id_field_in_ckan_dataset_if_missing(self, mock_super_import):
        '''
        Tests if contributorId field added if missing
        '''
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('test-portal','test-status')

        mock_super_import.return_value = True

        # run
        harvester.import_stage(harvest_obj)

        # check
        updated_content = json.loads(harvest_obj.content)
        harvest_config = json.loads(harvest_obj.source.config)
        expected_contributor_id = [harvest_config['contributorID']]

        contrib = next((extra for extra in updated_content.get('extras') if extra['key'] == 'contributorID'),
                       None)
        self.assertIsNotNone(contrib)
        self.assertEqual(contrib['value'], json.dumps(expected_contributor_id))

        mock_super_import.assert_called_once_with(harvest_obj)

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_add_contributor_id_in_ckan_dataset(self, mock_super_import):
        '''
        Tests if contributorId value added if it is not available yet
        '''
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('test-portal','test-status')
        harvest_obj_content = json.loads(harvest_obj.content)
        existing_contributor_id = 'http://dcat-ap.de/def/contributors/testContributor'
        extra = {'key': 'contributorID', 'value': json.dumps([existing_contributor_id])}
        harvest_obj_content['extras'] = [extra]
        harvest_obj.content = json.dumps(harvest_obj_content)
        mock_super_import.return_value = True

        # run
        harvester.import_stage(harvest_obj)
        updated_content = json.loads(harvest_obj.content)
        harvest_config = json.loads(harvest_obj.source.config)


        # check
        contrib = next((extra for extra in updated_content.get('extras') if extra['key'] == 'contributorID'),
                       None)
        self.assertIsNotNone(contrib)
        contributor_list = json.loads(contrib['value'])
        self.assertTrue(len(contributor_list) == 2)
        self.assertTrue(existing_contributor_id in contributor_list)
        self.assertTrue(harvest_config['contributorID'] in contributor_list)

        mock_super_import.assert_called_once_with(harvest_obj)

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_add_contributor_id_in_ckan_dataset_if_already_available(self, mock_super_import):
        '''
        Tests if contributorId value added if it is already available
        '''
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('test-portal','test-status')
        harvest_obj_content = json.loads(harvest_obj.content)
        harvest_config = json.loads(harvest_obj.source.config)
        extra = {'key': 'contributorID', 'value': json.dumps([harvest_config['contributorID']])}
        harvest_obj_content['extras'] = [extra]
        harvest_obj.content = json.dumps(harvest_obj_content)
        mock_super_import.return_value = True

        # run
        harvester.import_stage(harvest_obj)
        updated_content = json.loads(harvest_obj.content)

        # check
        contrib = next((extra for extra in updated_content.get('extras') if extra['key'] == 'contributorID'),
                       None)
        self.assertIsNotNone(contrib)
        contributor_list = json.loads(contrib['value'])
        self.assertTrue(len(contributor_list) == 1)
        self.assertTrue(harvest_config['contributorID'] in contributor_list)

        mock_super_import.assert_called_once_with(harvest_obj)

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

    @patch('ckanext.harvest.harvesters.base.HarvesterBase._get_user_name')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckan.model.Package.get')
    @patch("ckan.plugins.toolkit.get_action")
    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.rename_delete_dataset_with_id')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_custom_delete(self, mock_super_import, mock_deletion, mock_get_action,
                                  mock_model_package_get, mock_fuseki_delete_data_mqa,
                                  mock_fuseki_delete_data, mock_harvest_get_username):
        """
        Tests if the dataset deletion logic is independent of the base implementation and that
        the custom renaming logic from HarvestUtils is used.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'delete')

        mock_harvest_get_username.return_value = 'harvest'
        uri = 'http://ckan.govdata.de/dataset/317aa7ac-fd1d-49fe-8a43-63eb64d4392c'
        rdf = self._get_rdf(uri)
        mock_dcat_dataset_show = Mock()
        mock_dcat_dataset_show.return_value = rdf
        mock_get_action.return_value = mock_dcat_dataset_show

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        # run
        result = harvester.import_stage(harvest_obj)

        # check
        self.assertTrue(result)
        # no call to the base logic was made
        mock_super_import.assert_not_called()
        mock_deletion.assert_called_with('test-id')
        mock_harvest_get_username.assert_called_once_with()
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        mock_get_action.assert_has_calls([call('dcat_dataset_show')])
        mock_fuseki_delete_data.assert_called_with(URIRef(uri))
        mock_model_package_get.assert_not_called()
        mock_fuseki_delete_data_mqa.assert_called_with(URIRef(uri))

    @patch('ckanext.harvest.harvesters.base.HarvesterBase._get_user_name')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch("ckan.plugins.toolkit.get_action")
    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.handle_duplicates')
    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.rename_delete_dataset_with_id')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_handle_duplicates_accept(self, mock_super_import, mock_deletion, mock_handle_duplicates,
                                             mock_get_action, mock_fuseki_delete_data,
                                             mock_harvest_get_username):
        """
        Tests if the dataset duplicate detection logic is called and the super implementation of import_stage
        is called, if the import is accepted.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', '')
        # Import accepted
        mock_handle_duplicates.return_value = True

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        # run
        result = harvester.import_stage(harvest_obj)

        # check
        self.assertTrue(result)
        # no call to the custom delete logic was made
        mock_deletion.assert_not_called()
        mock_get_action.assert_not_called()
        mock_harvest_get_username.assert_not_called()
        mock_triplestore_is_available.assert_not_called()
        mock_fuseki_delete_data.assert_not_called()
        mock_handle_duplicates.assert_called_with(harvest_obj)
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
        mock_handle_duplicates.assert_called_with(harvest_obj)
        mock_save_object_error.assert_called_with(
            'Skipping importing dataset, because of duplicate detection!', harvest_obj, 'Import')
        mock_super_import.assert_not_called()

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

    @patch('ckanext.harvest.harvesters.base.HarvesterBase._get_user_name')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch("ckan.plugins.toolkit.get_action")
    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestUtils.rename_delete_dataset_with_id')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._read_datasets_from_db')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_no_resources_skip_dataset_exists(self, mock_super_import, mock_save_object_error,
                                                     mock_read_datasets_from_db, mock_deletion,
                                                     mock_get_action, mock_fuseki_delete_data,
                                                     mock_harvest_get_username):
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

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        # run
        harvester.import_stage(harvest_obj)

        # check _read_datasets_from_db is called with the guid of the harvest object
        mock_read_datasets_from_db.assert_called_once_with('guid-123')
        # check if error was saved
        self._assert_no_resources_error(harvest_obj, mock_save_object_error)
        # local dataset should be deleted
        mock_deletion.assert_called_once_with('test-id')
        mock_harvest_get_username.assert_not_called()
        mock_triplestore_is_available.assert_not_called()
        mock_get_action.assert_not_called()
        mock_fuseki_delete_data.assert_not_called()
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

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    @patch('ckan.model.Package.get')
    def test_harvesting_one_dataset_after_parse(self, mock_model_get, mock_shacl_validate,
                                                mock_fuseki_create_data_mqa, mock_fuseki_delete_data_mqa,
                                                mock_fuseki_create_data, mock_fuseki_delete_data,
                                                mock_fuseki_create_hi, mock_fuseki_delete_hi):
        """
        Test valid content in after_parsing() and check if correct methods are being called.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        maxrdf = self._get_max_rdf('metadata_max_only_valid_uris')
        rdf_parser = RDFParser()
        rdf_parser.parse(maxrdf, 'application/rdf+xml')
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')
        config = json.loads(harvest_obj.source.config)

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available
        org_id = "test-org-id"
        mock_model_get.return_value = Mock(owner_org=org_id)
        mock_validate_result = Mock(name='validate-result')
        mock_shacl_validate.return_value = mock_validate_result

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        mock_model_get.assert_called_once_with(harvest_obj.source.id)
        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEqual(len(error_msgs), 0)
        uri = next(rdf_parser._datasets())
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # check if delete dataset was called. Testdata has only one dataset
        mock_fuseki_delete_data.assert_called_once_with(uri)
        # check if create dataset was called
        mock_fuseki_create_data.assert_called_once_with(ANY, uri)
        # check if shacle validator was called
        mock_shacl_validate.assert_called_once_with(ANY, uri, org_id, config['contributorID'])
        # check if delete dataset was called.
        mock_fuseki_delete_data_mqa.assert_called_once_with(uri)
        # check if create dataset was called
        mock_fuseki_create_data_mqa.assert_called_once_with(mock_validate_result, uri)
        # check if delete harvest_info was called.
        mock_fuseki_delete_hi.assert_called_once_with(uri)
        # check if create harvest_info was called.
        mock_fuseki_create_hi.assert_called_once_with(ANY, uri)
        # check if harvest_info was called with the correct parameters
        # the order of the graph is random, so we to check values individually
        self._assert_rdf_harvest_info(mock_fuseki_create_hi.call_args_list, [uri], org_id,
                                      harvest_obj.source.id)

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATdeRDFHarvester._get_contributor_from_config')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    @patch('ckan.model.Package.get')
    def test_harvesting_one_dataset_after_parse_contributor_id_from_config(self, mock_model_get, mock_shacl_validate,
                                                                           mock_fuseki_create_data_mqa, mock_fuseki_delete_data_mqa,
                                                                           mock_fuseki_create_data, mock_fuseki_delete_data,
                                                                           mock_fuseki_create_hi, mock_fuseki_delete_hi,
                                                                           mock_get_contrib_from_config):
        """
        Test valid content in after_parsing() and check if correct contributor id is beeing used.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        maxrdf = self._get_max_rdf('metadata_max_only_valid_uris')
        rdf_parser = RDFParser()
        rdf_parser.parse(maxrdf, 'application/rdf+xml')
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')
        contributor_id = 'http://dcat-ap.de/def/contributors/testContributor'

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available
        org_id = "test-org-id"
        mock_model_get.return_value = Mock(owner_org=org_id)
        mock_get_contrib_from_config.return_value = contributor_id
        mock_validate_result = Mock(name='validate-result')
        mock_shacl_validate.return_value = mock_validate_result

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        mock_model_get.assert_called_once_with(harvest_obj.source.id)
        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEqual(len(error_msgs), 0)
        uri = next(rdf_parser._datasets())
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # check if delete dataset was called. Testdata has only one dataset
        mock_fuseki_delete_data.assert_called_once_with(uri)
        # check if create dataset was called
        mock_fuseki_create_data.assert_called_once_with(ANY, uri)
        # check if get contributor id from config was called
        mock_get_contrib_from_config.assert_called_once_with(harvest_obj.source.config)
        # check if shacle validator was called
        mock_shacl_validate.assert_called_once_with(ANY, uri, org_id, contributor_id)
        # check if delete dataset was called.
        mock_fuseki_delete_data_mqa.assert_called_once_with(uri)
        # check if create dataset was called
        mock_fuseki_create_data_mqa.assert_called_once_with(mock_validate_result, uri)
        # check if delete harvest_info was called.
        mock_fuseki_delete_hi.assert_called_once_with(uri)
        # check if create harvest_info was called.
        mock_fuseki_create_hi.assert_called_once_with(ANY, uri)
        # check if harvest_info was called with the correct parameters
        # the order of the graph is random, so we to check values individually
        self._assert_rdf_harvest_info(mock_fuseki_create_hi.call_args_list, [uri], org_id,
                                      harvest_obj.source.id)

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATdeRDFHarvester._get_contributor_from_config')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    @patch('ckan.model.Package.get')
    def test_harvesting_one_dataset_after_parse_no_contributor_id(self, mock_model_get, mock_shacl_validate,
                                                                  mock_fuseki_create_data_mqa, mock_fuseki_delete_data_mqa,
                                                                  mock_fuseki_create_data, mock_fuseki_delete_data,
                                                                  mock_fuseki_create_hi, mock_fuseki_delete_hi,
                                                                  mock_get_contrib_from_config):
        """
        Test valid content in after_parsing() and check if owner_org is used as fallback for missing contributor id.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        maxrdf = self._get_max_rdf('metadata_max_only_valid_uris')
        rdf_parser = RDFParser()
        rdf_parser.parse(maxrdf, 'application/rdf+xml')
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available
        org_id = "test-org-id"
        mock_model_get.return_value = Mock(owner_org=org_id)
        mock_get_contrib_from_config.return_value = None
        mock_validate_result = Mock(name='validate-result')
        mock_shacl_validate.return_value = mock_validate_result

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        mock_model_get.assert_called_once_with(harvest_obj.source.id)
        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEqual(len(error_msgs), 0)
        uri = next(rdf_parser._datasets())
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # check if delete dataset was called. Testdata has only one dataset
        mock_fuseki_delete_data.assert_called_once_with(uri)
        # check if create dataset was called
        mock_fuseki_create_data.assert_called_once_with(ANY, uri)
        # check if read contrib id was called
        mock_get_contrib_from_config.assert_called_once_with(harvest_obj.source.config)
        # check if shacle validator was called
        mock_shacl_validate.assert_called_once_with(ANY, uri, org_id, None)
        # check if delete dataset was called.
        mock_fuseki_delete_data_mqa.assert_called_once_with(uri)
        # check if create dataset was called
        mock_fuseki_create_data_mqa.assert_called_once_with(mock_validate_result, uri)
        # check if delete harvest_info was called.
        mock_fuseki_delete_hi.assert_called_once_with(uri)
        # check if create harvest_info was called.
        mock_fuseki_create_hi.assert_called_once_with(ANY, uri)
        # check if harvest_info was called with the correct parameters
        # the order of the graph is random, so we to check values individually
        self._assert_rdf_harvest_info(mock_fuseki_create_hi.call_args_list, [uri], org_id,
                                      harvest_obj.source.id)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    @patch('ckan.model.Package.get')
    def test_harvesting_invalid_uriref_after_parse(self, mock_model_get, mock_shacl_validate,
                                                   mock_fuseki_create_data_mqa, mock_fuseki_delete_data_mqa,
                                                   mock_fuseki_create_data, mock_fuseki_delete_data,
                                                   mock_fuseki_create_hi, mock_fuseki_delete_hi):
        """
        Check if datasets containing invalid URIRefs are skipped.
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        # metadata_max contains invalid tel URI for contact (has spaces)
        maxrdf = self._get_max_rdf('metadata_max')
        rdf_parser = RDFParser()
        rdf_parser.parse(maxrdf, 'application/rdf+xml')
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        mock_model_get.assert_called_once_with(harvest_obj.source.id)
        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # error should be returned
        self.assertEqual(len(error_msgs), 1)
        uri = next(rdf_parser._datasets())
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # Dataset should be removed, but not created again
        mock_fuseki_delete_data.assert_called_once_with(uri)
        mock_fuseki_create_data.assert_not_called()
        # validation should also be skipped
        mock_shacl_validate.assert_not_called()
        # and no validation results should be written, but existing should be removed
        mock_fuseki_delete_data_mqa.assert_called_once_with(uri)
        mock_fuseki_create_data_mqa.assert_not_called()
        # harvest_info should be removed
        mock_fuseki_delete_hi.assert_called_once_with(uri)
        # create harvest_info skipped
        mock_fuseki_create_hi.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    def test_harvesting_after_parse_triplestore_not_available(
            self, mock_shacl_validate, mock_fuseki_create_data_mqa, mock_fuseki_delete_data_mqa,
            mock_fuseki_create_data, mock_fuseki_delete_data, mock_fuseki_create_hi, mock_fuseki_delete_hi):
        """
        Test behaviour if triplestore is not available
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        rdf_parser = RDFParser()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = False
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEqual(len(error_msgs), 0)
        mock_triplestore_is_available.assert_called_once_with()
        # create dataset should not be called
        mock_fuseki_create_data.assert_not_called()
        # delete dataset should not be called
        mock_fuseki_delete_data.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_fuseki_delete_data_mqa.assert_not_called()
        mock_fuseki_create_data_mqa.assert_not_called()
        mock_fuseki_delete_hi.assert_not_called()
        mock_fuseki_create_hi.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    def test_harvesting_after_parse_rdf_parser_not_available(
            self, mock_shacl_validate, mock_fuseki_create_data_mqa, mock_fuseki_delete_data_mqa,
            mock_fuseki_create_data, mock_fuseki_delete_data, mock_fuseki_create_hi, mock_fuseki_delete_hi):
        """
        Test behaviour if RDF-Parser is not available
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        rdf_parser = None
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEqual(len(error_msgs), 0)
        mock_triplestore_is_available.assert_not_called()
        # create dataset should not be called
        mock_fuseki_create_data.assert_not_called()
        # delete dataset should not be called
        mock_fuseki_delete_data.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_fuseki_delete_data_mqa.assert_not_called()
        mock_fuseki_create_data_mqa.assert_not_called()
        mock_fuseki_delete_hi.assert_not_called()
        mock_fuseki_create_hi.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    @patch('ckan.model.Package.get')
    def test_harvesting_multiple_datasets_after_parse(
            self, mock_model_get, mock_shacl_validate, mock_fuseki_create_data_mqa,
            mock_fuseki_delete_data_mqa, mock_fuseki_create_data, mock_fuseki_delete_data,
            mock_fuseki_create_hi, mock_fuseki_delete_hi):
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
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        mock_model_get.return_value = Mock(owner_org="test-org-id")
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # check if no errors are returned
        self.assertEqual(len(error_msgs), 0)
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # check if delete dataset was called twice
        self.assertEqual(mock_fuseki_delete_data.call_count, len(uris))
        # check if create dataset was called twice
        self.assertEqual(mock_fuseki_create_data.call_count, len(uris))
        # check if shacl validator was called twice
        self.assertEqual(mock_shacl_validate.call_count, len(uris))
        # check if delete dataset was called twice
        self.assertEqual(mock_fuseki_delete_data_mqa.call_count, len(uris))
        # check if create dataset was called twice
        self.assertEqual(mock_fuseki_create_data_mqa.call_count, len(uris))
        # check if delete dataset was called twice
        self.assertEqual(mock_fuseki_delete_hi.call_count, len(uris))
        # check if create dataset was called twice
        self.assertEqual(mock_fuseki_create_hi.call_count, len(uris))

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    @patch('ckan.model.Package.get')
    def test_harvesting_multiple_datasets_invalid_uriref_after_parse(
            self, mock_model_get, mock_shacl_validate, mock_fuseki_create_data_mqa,
            mock_fuseki_delete_data_mqa, mock_fuseki_create_data, mock_fuseki_delete_data,
            mock_fuseki_create_hi, mock_fuseki_delete_hi):
        """
        Test multiple datasets in after_parsing() with some invalid URIs and check if only correctly parsed
        datasets are imported.
        """
        # prepare
        uris = [URIRef("http:// this is invalid"), URIRef("http://example.org/datasets/2"),
                URIRef("http://example.org/datasets/3"), URIRef("http://example.org/datasets/4")]
        g = Graph()
        for uri in uris:
            g.add((uri, RDF.type, self.DCAT.Dataset))
        invalid_res = URIRef("http://example.org/datasets/2/resource/{not-valid")
        g.add((invalid_res, RDF.type, self.DCAT.Distribution))
        g.add((uris[1], self.DCAT.distribution, invalid_res))
        # newlines and tabs are serialized, but will throw an error when the triple store API is called.
        # Check if import succeeds in case the API returns OK, to ensure consistent parser behaviour
        # when updating versions.
        # Skipping of datasets upon API call failure is checked in a separate test case.
        g.add((uris[2], self.DCAT.theme, URIRef("https://actually\ninvalid\tbut-serialized")))

        rdf_parser = RDFParser()
        rdf_parser.g = g
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')
        config = json.loads(harvest_obj.source.config)

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available
        org_id = "test-org-id"
        mock_model_get.return_value = Mock(owner_org=org_id)
        mock_validate_result = Mock(name='validate-result')
        mock_shacl_validate.return_value = mock_validate_result

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        mock_model_get.assert_called_once_with(harvest_obj.source.id)
        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # There should be two errors
        self.assertEqual(len(error_msgs), 2)
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # all datasets should be deleted
        self.assertEqual(mock_fuseki_delete_data.call_count, len(uris))
        # only datsets 3 and 4 should be imported
        self.assertEqual(mock_fuseki_create_data.call_count, 2)
        mock_fuseki_create_data.assert_any_call(ANY, uris[2])
        mock_fuseki_create_data.assert_any_call(ANY, uris[3])
        # check if shacle validator was called for datasets 3 and 4
        self.assertEqual(mock_shacl_validate.call_count, 2)
        mock_shacl_validate.assert_any_call(ANY, uris[2], org_id, config['contributorID'])
        mock_shacl_validate.assert_any_call(ANY, uris[3], org_id, config['contributorID'])
        # check if delete mqa storage called for all datasets
        self.assertEqual(mock_fuseki_delete_data_mqa.call_count, len(uris))
        for uri in uris:
            mock_fuseki_delete_data_mqa.assert_any_call(uri)
        # check if MQA result storage is handled correctly for datsets 3 and 4
        self.assertEqual(mock_fuseki_create_data_mqa.call_count, 2)
        mock_fuseki_create_data_mqa.assert_any_call(mock_validate_result, uris[2])
        mock_fuseki_create_data_mqa.assert_any_call(mock_validate_result, uris[3])
        # check if delete harvest_info was called.
        self.assertEqual(mock_fuseki_delete_hi.call_count, len(uris))
        for uri in uris:
            mock_fuseki_delete_hi.assert_any_call(uri)
        # check if create harvest_info was called.
        self.assertEqual(mock_fuseki_create_hi.call_count, 2)
        mock_fuseki_create_hi.assert_any_call(ANY, uris[2])
        mock_fuseki_create_hi.assert_any_call(ANY, uris[3])
        # check if create harvest_info was called with correct parameters
        # the order of the graph is random, so we to check values individually
        self._assert_rdf_harvest_info(mock_fuseki_create_hi.call_args_list, [uris[2], uris[3]], org_id,
                                      harvest_obj.source.id)

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    def test_harvesting_delete_error_msg_after_parse(
            self, mock_shacl_validate, mock_fuseki_create_data_mqa, mock_fuseki_delete_data_mqa,
            mock_fuseki_create_data, mock_fuseki_delete_data, mock_fuseki_create_hi, mock_fuseki_delete_hi):
        """
        Test SPARQLWrapper exception while deleting in method after_parsing().
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        maxrdf = self._get_max_rdf('metadata_max_only_valid_uris')
        rdf_parser = RDFParser()
        rdf_parser.parse(maxrdf, 'application/rdf+xml')
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        mock_fuseki_delete_data.side_effect = SPARQLWrapperException('500 Internal server error!')

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # check that one error is returned
        self.assertEqual(len(error_msgs), 1)
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # check if delete dataset was called. Testdata has only one dataset.
        mock_fuseki_delete_data.assert_called_once_with(next(rdf_parser._datasets()))
        # create dataset should not be called
        mock_fuseki_create_data.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_fuseki_delete_data_mqa.assert_not_called()
        mock_fuseki_create_data_mqa.assert_not_called()
        mock_fuseki_delete_hi.assert_not_called()
        mock_fuseki_create_hi.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    @patch('ckan.model.Package.get')
    def test_harvesting_create_error_msg_after_parse(
            self, mock_model_get, mock_shacl_validate, mock_fuseki_create_data_mqa,
            mock_fuseki_delete_data_mqa, mock_fuseki_create_data, mock_fuseki_delete_data,
            mock_fuseki_create_hi, mock_fuseki_delete_hi):
        """
        Test SPARQLWrapper exception while creating in method after_parsing().
        """
        # prepare
        harvester = DCATdeRDFHarvester()
        maxrdf = self._get_max_rdf('metadata_max_only_valid_uris')
        rdf_parser = RDFParser()
        rdf_parser.parse(maxrdf, 'application/rdf+xml')
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        mock_fuseki_create_data.side_effect = SPARQLWrapperException('500 Internal server error!')

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        mock_model_get.assert_called_once_with(harvest_obj.source.id)
        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # check that one error is returned
        self.assertEqual(len(error_msgs), 1)
        uri = next(rdf_parser._datasets())
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # check if delete dataset was called. Testdata has only one dataset.
        mock_fuseki_delete_data.assert_called_once_with(uri)
        # check if create dataset was called
        mock_fuseki_create_data.assert_called_once_with(ANY, uri)
        mock_shacl_validate.assert_not_called()
        mock_fuseki_delete_data_mqa.assert_called_once_with(uri)
        mock_fuseki_create_data_mqa.assert_not_called()
        mock_fuseki_delete_hi.assert_called_once_with(uri)
        mock_fuseki_create_hi.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate')
    @patch('ckan.model.Package.get')
    def test_check_if_datasets_skipped_when_resource_required(
            self, mock_model_get, mock_shacl_validate, mock_fuseki_create_data_mqa,
            mock_fuseki_delete_data_mqa, mock_fuseki_create_data, mock_fuseki_delete_data,
            mock_fuseki_create_hi, mock_fuseki_delete_hi):
        """
        Test datasets in after_parsing() with and without distributions when required_resource is true.
        """
        # prepare
        # uri[0] contains a valid distribution and should be imported
        # uri[1] does not contain a distribution and should be skipped
        uris = [URIRef('http://example.org/datasets/0'), URIRef('http://example.org/datasets/1')]
        g = Graph()
        for uri in uris:
            g.add((uri, RDF.type, self.DCAT.Dataset))
        g.add((uris[0], self.DCAT.distribution, URIRef('http://testUrl')))

        rdf_parser = RDFParser()
        rdf_parser.g = g
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        # set resources_required to true in config
        config = json.loads(harvest_obj.source.config)
        config['resources_required'] = True
        harvest_obj.source.config = json.dumps(config)

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available
        org_id = "test-org-id"
        mock_model_get.return_value = Mock(owner_org=org_id)
        mock_validate_result = Mock(name='validate-result')
        mock_shacl_validate.return_value = mock_validate_result

        # run
        rdf_parser_return, error_msgs = harvester.after_parsing(rdf_parser, harvest_obj)

        mock_model_get.assert_called_once_with(harvest_obj.source.id)
        # the parser should not have changed
        self.assertEqual(rdf_parser_return, rdf_parser)
        # There should be zero errors
        self.assertEqual(len(error_msgs), 0)
        # at the beginning of after_parsing and delete from triplestore for every dataset
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        # all datasets should be deleted
        self.assertEqual(mock_fuseki_delete_data.call_count, len(uris))
        mock_fuseki_delete_data.assert_any_call(uris[0])
        mock_fuseki_delete_data.assert_any_call(uris[1])
        # only datset 1 should be imported
        mock_fuseki_create_data.assert_called_once_with(ANY, uris[0])
        # check if shacle validator was called for dataset 0
        mock_shacl_validate.assert_called_once_with(ANY, uris[0], org_id, config['contributorID'])
        # check if delete mqa storage called for all datasets
        self.assertEqual(mock_fuseki_delete_data_mqa.call_count, len(uris))
        mock_fuseki_delete_data_mqa.assert_any_call(uris[0])
        mock_fuseki_delete_data_mqa.assert_any_call(uris[1])
        # check if MQA result storage is handled correctly for datsets 0
        mock_fuseki_create_data_mqa.assert_called_once_with(mock_validate_result, uris[0])
        # check if delete harvest_info was called.
        self.assertEqual(mock_fuseki_delete_hi.call_count, len(uris))
        mock_fuseki_delete_hi.assert_any_call(uris[0])
        mock_fuseki_delete_hi.assert_any_call(uris[1])
        # check if create harvest_info was called.
        mock_fuseki_create_hi.assert_called_once_with(ANY, uris[0])
        # check if create harvest_info was called with correct parameters
        # the order of the graph is random, so we to check values individually
        self._assert_rdf_harvest_info(mock_fuseki_create_hi.call_args_list, [uris[0]],
                                      org_id, harvest_obj.source.id)

    @patch('ckanext.harvest.harvesters.base.HarvesterBase._get_user_name')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckan.model.Package.get')
    @patch("ckan.plugins.toolkit.get_action")
    def test_delete_dataset_in_triplestore(self, mock_get_action, mock_model_package_get,
                                           mock_fuseki_delete_hi, mock_fuseki_delete_data_mqa,
                                           mock_fuseki_delete_data, mock_harvest_get_username):
        """
        Tests if the dataset with the given ID will be deleted in the triplestore.
        """
        # prepare
        harvester = DCATdeRDFHarvester()

        mock_harvest_get_username.return_value = 'harvest'
        uri = 'http://ckan.govdata.de/dataset/317aa7ac-fd1d-49fe-8a43-63eb64d4392c'
        rdf = self._get_rdf(uri)
        mock_dcat_dataset_show = Mock()
        mock_dcat_dataset_show.return_value = rdf
        mock_get_action.return_value = mock_dcat_dataset_show

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'delete')

        # run
        harvester._delete_dataset_in_triplestore(harvest_obj)

        # check
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        mock_get_action.assert_has_calls([call('dcat_dataset_show')])
        mock_fuseki_delete_data.assert_called_with(URIRef(uri))
        mock_model_package_get.assert_not_called()
        mock_fuseki_delete_data_mqa.assert_called_with(URIRef(uri))
        mock_fuseki_delete_hi.assert_called_once_with(URIRef(uri))

    @patch('ckanext.harvest.harvesters.base.HarvesterBase._get_user_name')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch("ckan.plugins.toolkit.get_action")
    def test_delete_dataset_in_triplestore_RDFParserException(
            self, mock_get_action, mock_fuseki_delete_hi, mock_fuseki_delete_data_mqa,
            mock_fuseki_delete_data, mock_harvest_get_username):
        """
        Tests if the dataset with the given ID will be deleted in the triplestore.
        """
        # prepare
        harvester = DCATdeRDFHarvester()

        mock_harvest_get_username.return_value = 'harvest'
        mock_dcat_dataset_show = Mock()
        # Invalid rdf content raises expected RDFParserException
        mock_dcat_dataset_show.return_value = 'invalid rdf content'
        mock_get_action.return_value = mock_dcat_dataset_show

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'delete')

        # run
        harvester._delete_dataset_in_triplestore(harvest_obj)

        # check
        mock_triplestore_is_available.assert_called_once_with()
        mock_get_action.assert_has_calls([call('dcat_dataset_show')])
        mock_fuseki_delete_data.assert_not_called()
        mock_fuseki_delete_data_mqa.assert_not_called()
        mock_fuseki_delete_hi.assert_not_called()

    @patch('ckanext.harvest.harvesters.base.HarvesterBase._get_user_name')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch("ckan.plugins.toolkit.get_action")
    def test_delete_dataset_in_triplestore_SPARQLWrapperException(
            self, mock_get_action, mock_fuseki_delete_hi, mock_fuseki_delete_data_mqa,
            mock_fuseki_delete_data, mock_harvest_get_username):
        """
        Tests if the dataset with the given ID will be deleted in the triplestore.
        """
        # prepare
        harvester = DCATdeRDFHarvester()

        mock_harvest_get_username.return_value = 'harvest'
        uri = 'http://ckan.govdata.de/dataset/317aa7ac-fd1d-49fe-8a43-63eb64d4392c'
        rdf = self._get_rdf(uri)
        mock_dcat_dataset_show = Mock()
        mock_dcat_dataset_show.return_value = rdf
        mock_get_action.return_value = mock_dcat_dataset_show

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available
        # Raise expected SPARQLWrapperException
        mock_fuseki_delete_data.side_effect = SPARQLWrapperException('test error')

        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'delete')

        # run
        harvester._delete_dataset_in_triplestore(harvest_obj)

        # check
        mock_triplestore_is_available.assert_has_calls([call(), call()])
        mock_get_action.assert_has_calls([call('dcat_dataset_show')])
        mock_fuseki_delete_data.assert_called_with(URIRef(uri))
        mock_fuseki_delete_data_mqa.assert_not_called()
        mock_fuseki_delete_hi.assert_not_called()

    @patch('ckanext.harvest.harvesters.base.HarvesterBase._get_user_name')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch("ckan.plugins.toolkit.get_action")
    def test_delete_dataset_in_triplestore_dcat_dataset_show_unexpected_exception(
            self, mock_get_action, mock_fuseki_delete_hi, mock_fuseki_delete_data_mqa,
            mock_fuseki_delete_data, mock_harvest_get_username):
        """
        Tests if the dataset with the given ID will be deleted in the triplestore.
        """
        # prepare
        harvester = DCATdeRDFHarvester()

        mock_harvest_get_username.return_value = 'harvest'
        mock_dcat_dataset_show = Mock()
        # Raise unexpected KeyError
        mock_dcat_dataset_show.side_effect = KeyError('test error')
        mock_get_action.return_value = mock_dcat_dataset_show

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'delete')

        # run
        with self.assertRaises(KeyError) as cm:
            harvester._delete_dataset_in_triplestore(harvest_obj)

        # check
        mock_triplestore_is_available.assert_called_once_with()
        mock_get_action.assert_has_calls([call('dcat_dataset_show')])
        mock_fuseki_delete_data.assert_not_called()
        mock_fuseki_delete_data_mqa.assert_not_called()
        mock_fuseki_delete_hi.assert_not_called()

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATdeRDFHarvester._get_existing_dataset_uris_from_triplestore')
    @patch('ckan.model.Package.get')
    def test_delete_deprecated_datasets_from_triplestore(self, mock_package_get, mock_get_uris,
                                                         mock_triplestore_delete_ds, mock_triplestore_delete_mqa, mock_triplestore_delete_hi):
        """ Check if the functions to delete an URI in all triplestore datastores are called properly """

        uris_db_marked_as_deleted = ["URI-3"]
        harvested_uris = ["URI-1", "URI-2", "URI-3"]
        existing_uris = ["URI-0", "URI-1", "URI-2"]
        owner_org = 'owner-org-1'

        mock_package_get.return_value = Mock(name='package-mock', owner_org=owner_org)
        mock_get_uris.return_value = existing_uris
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester = DCATdeRDFHarvester()
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        harvester._delete_deprecated_datasets_from_triplestore(harvested_uris, uris_db_marked_as_deleted,
                                                               harvest_obj)

        mock_triplestore_is_available.assert_called_once_with()
        mock_package_get.assert_called_once_with(harvest_obj.source.id)
        mock_get_uris.assert_called_once_with(harvest_obj.source.id)
        mock_triplestore_delete_ds.assert_called_once_with(existing_uris[0])
        mock_triplestore_delete_mqa.assert_called_once_with(existing_uris[0])
        mock_triplestore_delete_hi.assert_called_once_with(existing_uris[0])

    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_harvest_info')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATdeRDFHarvester._get_existing_dataset_uris_from_triplestore')
    @patch('ckan.model.Package.get')
    def test_delete_deprecated_datasets_from_triplestore_no_source(self, mock_package_get, mock_get_uris,
                                                                   mock_triplestore_delete_ds, mock_triplestore_delete_mqa, mock_triplestore_delete_hi):
        """
        Test behaviour if owner org is not found: Deletes dataset from all triple stores as with owner org,
        because owner org is only used for logging
        """

        uris_db_marked_as_deleted = ["URI-3"]
        harvested_uris = ["URI-1", "URI-2", "URI-3"]
        existing_uris = ["URI-0", "URI-1", "URI-2"]

        mock_package_get.return_value = None
        mock_get_uris.return_value = existing_uris
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        mock_triplestore_is_available = Mock(name='triplestore-is-available')
        mock_triplestore_is_available.return_value = True
        harvester = DCATdeRDFHarvester()
        harvester.triplestore_client.is_available = mock_triplestore_is_available

        harvester._delete_deprecated_datasets_from_triplestore(harvested_uris, uris_db_marked_as_deleted,
                                                               harvest_obj)

        mock_triplestore_is_available.assert_called_once_with()
        mock_package_get.assert_called_once_with(harvest_obj.source.id)
        mock_get_uris.assert_called_once_with(harvest_obj.source.id)
        mock_triplestore_delete_ds.assert_called_once_with(existing_uris[0])
        mock_triplestore_delete_mqa.assert_called_once_with(existing_uris[0])
        mock_triplestore_delete_hi.assert_called_once_with(existing_uris[0])

    @patch('SPARQLWrapper.Wrapper.QueryResult.convert')
    @patch('ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.select_datasets_in_triplestore_harvest_info')
    def test_get_existing_dataset_uris_from_triplestore(self, mock_select_datatsets_ts, mock_convert):
        """ Test if select-request to triplestore is send properly and response is returned correctly """

        test_owner_org = "org-1"
        expected_result = ["URI-1", "URI-2"]
        raw_result = {"results": {"bindings": [{"s": {"value": expected_result[0]}}, {"s": {"value": expected_result[1]}}]}}

        mock_select_datatsets_ts.return_value = QueryResult(None)
        mock_convert.return_value = raw_result

        harvester = DCATdeRDFHarvester()
        result = harvester._get_existing_dataset_uris_from_triplestore(test_owner_org)

        mock_select_datatsets_ts.assert_called_once_with(GET_URIS_FROM_HARVEST_INFO_QUERY % {
            'owner_org_or_source_id': test_owner_org})
        self.assertEqual(result, expected_result)

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.HarvestObject')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATdeRDFHarvester._delete_deprecated_datasets_from_triplestore')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.model')
    def test_mark_datasets_for_deletion_dataset_to_delete(self, mock_model, mock_delete_deprecated_datasets,
                                                          mock_harvest_object):
        """ Check if the functions to delete deprecated datasets is called properly """

        packages_in_db = [('id-3', "URI-3"), ('id-4', "URI-4"), ('id-5', "URI-5")]
        uris_db_marked_as_deleted = [guid for package_id, guid in packages_in_db[1:]]
        # Mocking attribute 'id' with side_effect doesn't work. Possibly because the attribute 'id' isn't
        # declared for the HarvestObject class
        # harvest_obj_property_mock = PropertyMock(side_effect=uris_db_marked_as_deleted)
        # type(mock_harvest_object).id = harvest_obj_property_mock
        harvest_object_id = 'obj.id'
        mock_harvest_object.return_value.id = harvest_object_id
        harvested_uris = ["URI-1", "URI-2", "URI-3"]
        mock_query_result = Mock(name='query-result')
        mock_query_result.join().outerjoin().filter().filter().filter().filter.return_value = packages_in_db
        mock_query = Mock(name='query')
        mock_update_harvest_obj = Mock(name='update-harvest-obj')
        mock_query.side_effect = [Mock(name='subquery'), mock_query_result, mock_update_harvest_obj,
                                  mock_update_harvest_obj]
        mock_model.Session.query = mock_query
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'test-status')

        harvester = DCATdeRDFHarvester()
        object_ids = harvester._mark_datasets_for_deletion(harvested_uris, harvest_obj)

        # the values should be compared, but the side_effect isn't working
        self.assertEqual(object_ids, [harvest_object_id for x in uris_db_marked_as_deleted])
        self.assertEqual(mock_harvest_object.call_count, len(uris_db_marked_as_deleted))
        # subquery, query, 2 x update harvest_obj
        self.assertEqual(mock_query.call_count, 4)
        mock_delete_deprecated_datasets.assert_called_once_with(
            set(harvested_uris), set(uris_db_marked_as_deleted), harvest_obj)

    @patch('ckanext.dcat.harvesters.DCATRDFHarvester._mark_datasets_for_deletion')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.HarvestObject')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATdeRDFHarvester._delete_deprecated_datasets_from_triplestore')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.model')
    def test_mark_datasets_for_deletion_no_harvested_portal(self, mock_model,
                                                          mock_delete_deprecated_datasets,
                                                          mock_harvest_object,
                                                          mock_super_mark_datasets_for_deletion):
        """ Check if the functions to delete deprecated datasets are called properly
            when no harvested portal is configured in the harvest source. """

        packages_in_db = [('id-3', "URI-3"), ('id-4', "URI-4"), ('id-5', "URI-5")]
        uris_db_marked_as_deleted = [guid for package_id, guid in packages_in_db[1:]]
        ids_db_marked_as_deleted = [package_id for package_id, guid in packages_in_db[1:]]
        harvested_uris = ["URI-1", "URI-2", "URI-3"]
        mock_super_mark_datasets_for_deletion.return_value = ids_db_marked_as_deleted
        mock_query = Mock(name='query')
        mock_query.return_value.filter.return_value = uris_db_marked_as_deleted
        mock_model.Session.query = mock_query

        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy(None, 'test-status')

        harvester = DCATdeRDFHarvester()
        result = harvester._mark_datasets_for_deletion(harvested_uris, harvest_obj)

        self.assertEqual(result, ids_db_marked_as_deleted)
        self.assertEqual(mock_query.call_count, 1)
        mock_super_mark_datasets_for_deletion.assert_called_once_with(harvested_uris, harvest_obj)
        mock_delete_deprecated_datasets.assert_called_once_with(
            set(harvested_uris), uris_db_marked_as_deleted, harvest_obj)