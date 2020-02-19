#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import unittest

from ckanext.dcatde.harvesters.dcatde_rdf import DCATdeRDFHarvester
from mock import patch, Mock, ANY

from ckantoolkit.tests import helpers

class TestDCATdeRDFHarvester(unittest.TestCase):
    """
    Test class for the DCATdeRDFHarvester
    """

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
    def _prepare_license_obj():
        harvest_obj = TestDCATdeRDFHarvester._get_harvest_obj_dummy('testportal', 'license-fallback')
        obj_content = json.dumps({
            'id': 'test-id',
            'name': 'test-name',
            'resources': [
                {'uri': 'http://example.com/no-license-ä'},
                {'uri': 'http://example.com/license', 'license': 'foo'},
            ]
        })
        harvest_obj.content = obj_content
        return harvest_obj

    def _assert_resource_license(self, harvest_obj, expected_license):
        updated_content = json.loads(harvest_obj.content)
        updated_resources = updated_content.get('resources')
        self.assertEquals(updated_resources[0]['license'], expected_license)
        self.assertEquals(updated_resources[1]['license'], u'foo')

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
        harvest_obj = TestDCATdeRDFHarvester._prepare_license_obj()

        # run
        harvester.import_stage(harvest_obj)

        # check
        self._assert_resource_license(harvest_obj, u'http://dcat-ap.de/def/licenses/other-closed')

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    @helpers.change_config('ckanext.dcatde.harvest.default_license', 'test-license')
    def test_license_fallback_with_cfg(self, mock_super_import):
        # prepare
        harvester = DCATdeRDFHarvester()
        harvest_obj = TestDCATdeRDFHarvester._prepare_license_obj()

        # run
        harvester.import_stage(harvest_obj)

        # check
        self._assert_resource_license(harvest_obj, u'test-license')

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
        mock_read_datasets_from_db.return_value = False

        # run
        harvester.import_stage(harvest_obj)

        # check _read_datasets_from_db is called with the guid of the harvest object
        mock_read_datasets_from_db.assert_called_once_with('guid-123')
        # check (do not require exact string message but look for keywords)
        mock_save_object_error.assert_called_once_with(
            ANY, harvest_obj, 'Import'
        )
        message = mock_save_object_error.call_args[0][0].lower()
        self.assertTrue('skip' in message)
        self.assertTrue('no resources' in message)
        # dataset should be skipped
        mock_super_import.assert_not_called()

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._read_datasets_from_db')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_no_resources_skip_dataset_exists(self, mock_super_import, mock_save_object_error,
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
        mock_read_datasets_from_db.return_value = True

        # run
        harvester.import_stage(harvest_obj)

        # check _read_datasets_from_db is called with the guid of the harvest object
        mock_read_datasets_from_db.assert_called_once_with('guid-123')
        # do not save object error because import is accepted
        mock_save_object_error.assert_not_called()

        mock_super_import.assert_called_once_with(harvest_obj)

    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._read_datasets_from_db')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester._save_object_error')
    @patch('ckanext.dcatde.harvesters.dcatde_rdf.DCATRDFHarvester.import_stage')
    def test_import_no_resources_empty_list_skip(self, mock_super_import, mock_save_object_error,
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
        harvest_object_content = json.loads(harvest_obj.content)
        harvest_object_content['resources'] = []
        harvest_obj.content = json.dumps(harvest_object_content)
        mock_read_datasets_from_db.return_value = False

        # run
        harvester.import_stage(harvest_obj)

        # check _read_datasets_from_db is called with the guid of the harvest object
        mock_read_datasets_from_db.assert_called_once_with('guid-123')
        # check (do not require exact string message but look for keywords)
        mock_save_object_error.assert_called_once_with(
            ANY, harvest_obj, 'Import'
        )
        message = mock_save_object_error.call_args[0][0].lower()
        self.assertTrue('skip' in message)
        self.assertTrue('no resources' in message)
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
