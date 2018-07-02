#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import unittest

from ckanext.dcatde.harvesters.dcatde_rdf import DCATdeRDFHarvester
from mock import patch, Mock


class TestDCATdeRDFHarvester(unittest.TestCase):
    """
    Test class for the DCATdeRDFHarvester
    """

    @classmethod
    def _get_harvest_obj_dummy(cls, portal, status):
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
        harvest_obj = Mock(content=obj_content, package_id='test-id',
                           source=harvest_src, extras=[harvest_extra])

        return harvest_obj

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
            'Skipping importing dataset %s, because of duplicate detection!' % (dataset_dict['name']),
            harvest_obj, 'Import')
        mock_super_import.assert_not_called()
