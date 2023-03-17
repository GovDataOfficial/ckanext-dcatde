#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import unittest

from ckanext.dcatde.harvesters.harvest_utils import HarvestUtils
from ckanext.harvest.model import HarvestObject
from mock import call, patch, Mock, ANY

class DummySource():

    def __init__(self, title, config):
        self.title = title
        self.config = config

class TestHarvestUtils(unittest.TestCase):
    """
    Test class for HarvestUtils
    """

    @classmethod
    def _mock_api_context(cls):
        return {'ignore_auth': True, 'model': ANY, 'session': ANY, 'user': u'harvest',
                'api_version': 1}

    def test_create_new_name_for_deletion(self):
        """
        Asserts that a new dataset name is created by create_new_name_for_deletion.
        """
        test_name = "test-name"
        new_name = HarvestUtils.create_new_name_for_deletion(test_name)
        self.assertNotEqual(test_name, new_name, "Dataset name not changed")

    @patch('ckan.plugins.toolkit.get_action')
    def test_rename_datasets_before_delete(self, mock_get_action):
        # prepare
        package1 = {
            'id': 'abc',
            'name': 'one'
        }
        package2 = {
            'id': 'efg',
            'name': 'two'
        }

        mock_action_methods = Mock("action-methods")
        # 3) get_action('package_update')(context, package)
        mock_get_action.return_value = mock_action_methods

        deprecated_package_dicts = [package1, package2]

        oldnames = []
        for pkg in deprecated_package_dicts:
            oldnames.append(pkg['name'])

        # execute
        HarvestUtils.rename_datasets_before_delete(deprecated_package_dicts)

        # verify
        self.assertEqual(mock_get_action.call_count, 1)
        mock_get_action.assert_any_call("package_update")
        self.assertEqual(mock_action_methods.call_count, len(deprecated_package_dicts))

        expected_action_calls = []
        for pkg in deprecated_package_dicts:
            self.assertTrue(pkg['name'] not in oldnames,
                            "Expected dataset " + pkg['id'] + " to be renamed, but wasn't.")

            expected_action_calls.append(call(TestHarvestUtils._mock_api_context(), pkg))

        mock_action_methods.assert_has_calls(expected_action_calls, any_order=True)

    @patch('ckan.plugins.toolkit.get_action')
    def test_delete_packages(self, mock_get_action):
        # prepare
        package1_id = 'abc'
        package2_id = 'efg'

        mock_action_methods = Mock("action-methods")
        # 3) get_action('package_delete')(context, {'id': to_delete_id})
        mock_get_action.return_value = mock_action_methods

        package_ids_to_delete = [package1_id, package2_id]

        # execute
        HarvestUtils.delete_packages(package_ids_to_delete)

        # verify
        self.assertEqual(mock_get_action.call_count, 1)
        mock_get_action.assert_any_call("package_delete")
        self.assertEqual(mock_action_methods.call_count, len(package_ids_to_delete))
        expected_action_calls_original = []
        for to_delete_id in package_ids_to_delete:
            expected_action_calls_original.append(
                call(TestHarvestUtils._mock_api_context(),
                     {'id': to_delete_id}))
        mock_action_methods.assert_has_calls(expected_action_calls_original, any_order=True)

    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestObject')
    @patch('ckanext.dcatde.harvesters.harvest_utils.model')
    @patch('ckan.plugins.toolkit.get_action')
    def test_rename_delete_dataset_with_id(self, mock_get_action, mock_model, mock_harvest_object):
        mock_action_methods = Mock("action-methods")
        # always return a pseudo-package from API methods
        mock_action_methods.return_value = {'id': 'test', 'name': 'package'}
        mock_get_action.return_value = mock_action_methods

        mock_query = Mock(name='query')
        mock_update_harvest_obj = Mock(name='update-harvest-obj')
        mock_query.side_effect =  mock_update_harvest_obj
        mock_model.Session.query = mock_query

        HarvestUtils.rename_delete_dataset_with_id('test')

        # check if the expected API calls were made (show to get the package name)
        self.assertEqual(mock_get_action.call_count, 3)
        mock_get_action.assert_any_call("package_show")
        mock_get_action.assert_any_call("package_update")
        mock_get_action.assert_any_call("package_delete")

        self.assertEqual(mock_action_methods.call_count, 3)
        expected_action_calls = []
        # call with ID only (show)
        expected_action_calls.append(
            call(TestHarvestUtils._mock_api_context(),
                 {'id': 'test'}))
        # one call with a name (update)
        expected_action_calls.append(
            call(TestHarvestUtils._mock_api_context(),
                 {'id': 'test', 'name': ANY}))
        # and only ID again (delete)
        expected_action_calls.append(expected_action_calls[0])
        mock_action_methods.assert_has_calls(expected_action_calls)

        # Check if the function to update harvest objects (current=false) is called properly before delete
        self.assertEqual(mock_query.call_count, 1)

        # finally check if the name passed to the update call was different
        self.assertTrue(
            call(TestHarvestUtils._mock_api_context(),
                 {'id': 'test', 'name': 'package'}) not in mock_action_methods.call_args_list,
            "Name was not updated")

    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestObject')
    @patch('ckanext.dcatde.harvesters.harvest_utils.model')
    @patch('ckan.plugins.toolkit.get_action')
    def test_rename_delete_dataset_with_id_if_name_updated(self, mock_get_action, mock_model, mock_harvest_object):
        '''Tests if dataset with id deleted if name was not updated'''
        mock_action_methods = Mock("action-methods")
        # always return a pseudo-package from API methods
        #mock_action_methods.return_value = {'id': 'test', 'name': 'Package'}
        mock_action_methods.side_effect = [{'id': 'test', 'name': 'Package'},
            Exception('Update action failed'),{'id': 'test', 'name': 'Package'}]
        mock_get_action.return_value = mock_action_methods

        mock_query = Mock(name='query')
        mock_update_harvest_obj = Mock(name='update-harvest-obj')
        mock_query.side_effect =  mock_update_harvest_obj
        mock_model.Session.query = mock_query

        HarvestUtils.rename_delete_dataset_with_id('test')

        # check if the expected API calls were made (show to get the package name)
        self.assertEqual(mock_get_action.call_count, 3)
        mock_get_action.assert_any_call("package_show")
        mock_get_action.assert_any_call("package_update")
        mock_get_action.assert_any_call("package_delete")

        self.assertEqual(mock_action_methods.call_count, 3)
        expected_action_calls = []
        # call with ID only (show)
        expected_action_calls.append(
            call(TestHarvestUtils._mock_api_context(),
                 {'id': 'test'}))
        # one call with a name (update)
        expected_action_calls.append(
            call(TestHarvestUtils._mock_api_context(),
                 {'id': 'test', 'name': ANY}))
        # and only ID again (delete)
        expected_action_calls.append(expected_action_calls[0])
        mock_action_methods.assert_has_calls(expected_action_calls)

        # Check if the function to update harvest objects (current=false) is called properly before delete
        self.assertEqual(mock_query.call_count, 1)

    def test_compare_harvester_priorities(self):
        # Remote priority is higher than local priority
        local_harvester_config = { 'priority': 5 }
        remote_harvester_config = { 'priority': 10 }
        result = HarvestUtils.compare_harvester_priorities(local_harvester_config, remote_harvester_config)
        self.assertTrue(result, "Expected remote priority to be higher")

        # Local priority is higher than local priority
        local_harvester_configs = [
            { 'priority': 10 },
            { 'priority': '10' } # Priority as String: it should be parsed to an Integer
        ]
        remote_harvester_config = { 'priority': 5 }
        for config in local_harvester_configs:
            result = HarvestUtils.compare_harvester_priorities(config, remote_harvester_config)
            self.assertFalse(result, "Expected local priority to be higher!")

        # Both priorities are the same (default should be 0)
        local_harvester_configs = [
            { 'priority': 0  },
            { 'priority': 'invalid10'} # Invalid priority: default priority should be used as fallback
        ]
        remote_harvester_config = {}
        for config in local_harvester_configs:
            result = HarvestUtils.compare_harvester_priorities(config, remote_harvester_config)
            self.assertFalse(result, "Expected priority to be equal!")

@patch('ckanext.dcatde.harvesters.harvest_utils._get_harvester_config_from_db')
@patch('ckanext.dcatde.harvesters.harvest_utils.HarvestObject')
class TestHandleDuplicates(unittest.TestCase):
    """
    Test class for handling duplicates
    """

    @classmethod
    def setUpClass(cls):
        cls.local_modified = '2017-08-14T10:00:00.000'
        cls.remote_modified = '2017-08-15T10:00:00+02:00'
        cls.local_newer_modified = '2017-08-17T10:00:00.000'
        cls.remote_source = DummySource('DummyHarvester', '{"priority": 0 }')
        cls.mock_package_delete_action = Mock("package_delete")
        cls.mock_update_harvest_obj = Mock(name='update-harvest-obj')
        cls.mock_query = Mock(name='query')
        cls.mock_query.side_effect =  cls.mock_update_harvest_obj
        cls.mock_model = patch('ckanext.dcatde.harvesters.harvest_utils.model').start()
        cls.mock_model.Session.query = cls.mock_query
        cls.mock_get_action = patch('ckan.plugins.toolkit.get_action').start()
        cls.mock_get_action.side_effect = cls.mock_action_methods

    @classmethod
    def tearDownClass(cls):
        patch.stopall()

    @classmethod
    def mock_action_methods(cls, action):
        if action == 'package_search':
            return cls.package_search_action
        if action == 'package_delete':
            return cls.mock_package_delete_action

    @classmethod
    def package_search_action(cls, context, data_dict):
        if data_dict["q"] == 'identifier:"hasone"':
            return {
                       'count': 1,
                       'results': [{
                           'id': 'hasone-local',
                           'name': 'hasone-name',
                           'extras': [{'key': 'modified', 'value': cls.local_modified},
                                      {'key': 'metadata_harvested_portal', 'value': 'harvest1'},
                                      {'key': 'harvest_source_id', 'value': 'source_id1'}]
                       }]
                   }
        elif data_dict["q"] == 'identifier:"nodata"':
            return {
                       'count': 0
                   }
        elif data_dict["q"] == 'identifier:"multiple"':
            return {
                       'count': 2,
                       'results': [{
                           'id': 'one-local',
                           'name': 'one-name',
                           'extras': [{'key': 'modified', 'value': cls.local_modified}]
                       }, {
                           'id': 'two-local',
                           'name': 'two-name',
                           'extras': [{'key': 'modified', 'value': cls.local_modified}]
                       }]
                   }
        elif data_dict["q"] == 'identifier:"newer"':
            return {
                       'count': 1,
                       'results': [{
                           'id': 'newer-local',
                           'name': 'newer-name',
                           'extras': [{'key': 'modified', 'value': cls.local_newer_modified}]
                       }]
                   }
        elif data_dict["q"] == 'identifier:"with-guid"' and data_dict["fq"] == '-guid:"1234567890"':
            return {
                        'count': 1,
                        'results': [{
                            'id': 'with-guid-local',
                            'name': 'with-guid-name',
                            'identifier': 'with-guid',
                            'extras': [{'key': 'modified', 'value': cls.local_modified}]
                        }]
                   }
        elif data_dict["q"] == 'identifier:"multiple-local-newer"':
            return {
                       'count': 2,
                       'results': [{
                           'id': 'one-local',
                           'name': 'one-name',
                           'extras': [{'key': 'modified', 'value': cls.local_newer_modified}]
                       }, {
                           'id': 'two-local',
                           'name': 'two-name',
                           'extras': [{'key': 'modified', 'value': cls.local_modified}]
                       }]
                   }
        elif data_dict["q"] == 'identifier:"multiple-without-modified-date"':
            return {
                       'count': 2,
                       'results': [{
                           'id': 'one-local',
                           'name': 'one-name',
                           'metadata_modified': cls.local_newer_modified,
                           'extras': []
                       }, {
                           'id': 'two-local',
                           'name': 'two-name',
                           'metadata_modified': cls.local_modified,
                           'extras': []
                       }]
                   }
        else:
            raise AssertionError('Unexpected query!')

    def tearDown(self):
        self.mock_query.reset_mock()
        self.mock_package_delete_action.reset_mock()
        self.mock_get_action.reset_mock()

    def test_handle_duplicates_remote_is_newer(self, mock_harvest_object,
                                               mock_get_harvest_config):
        '''Tests if remote dataset, which is newer is accepted'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.remote_modified,
                    'identifier': 'hasone'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # execute
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.assertEqual(self.mock_package_delete_action.call_count, 1)
        self.mock_package_delete_action.assert_called_with(
            TestHarvestUtils._mock_api_context(), {'id': 'hasone-local'})
        self.assertEqual(self.mock_query.call_count, 1)
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_has_multiple_duplicates(self, mock_harvest_object,
                                                              mock_get_harvest_config):
        '''Tests if remote dataset, which has multiple duplicates is accepted'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.remote_modified,
                    'identifier': 'multiple'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # execute
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.assertEqual(self.mock_package_delete_action.call_count, 2)
        self.mock_package_delete_action.assert_has_calls([
            call(TestHarvestUtils._mock_api_context(), {'id': 'one-local'}),
            call(TestHarvestUtils._mock_api_context(), {'id': 'two-local'})],
            any_order=True)
        self.assertEqual(self.mock_query.call_count, 1)
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_newer_no_local(self, mock_harvest_object,
                                                     mock_get_harvest_config):
        '''Tests if remote dataset, which is newer - and no local dataset,
            is accepted'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.remote_modified,
                    'identifier': 'nodata'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # execute
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 1)
        self.mock_get_action.assert_has_calls([call("package_search")])
        self.mock_package_delete_action.assert_not_called()
        self.mock_query.assert_not_called()
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_without_id(self, mock_harvest_object,
                                                 mock_get_harvest_config):
        '''Tests if remote dataset without ID is accepted'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.remote_modified,
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # execute
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.mock_get_action.assert_not_called()
        self.mock_package_delete_action.assert_not_called()
        self.mock_query.assert_not_called()
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_without_timestamp_local_has_one(self,
                                                                      mock_harvest_object,
                                                                      mock_get_harvest_config):
        '''Tests if remote dataset without timestamp but local has older one is rejected'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'identifier': 'hasone'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # verify
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.mock_package_delete_action.assert_not_called()
        self.assertEqual(self.mock_query.call_count, 1)
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_without_timestamp_same_harvester(self,
                                                                       mock_harvest_object,
                                                                       mock_get_harvest_config):
        '''Tests if remote dataset without timestamp, but same harvester is rejected'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'identifier': 'hasone',
                    'metadata_harvested_portal': 'harvest1'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # verify
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.mock_package_delete_action.assert_not_called()
        self.assertEqual(self.mock_query.call_count, 1)
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_local_is_newer(self, mock_harvest_object,
                                              mock_get_harvest_config):
        '''Tests if remote dataset, which older is rejected'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.remote_modified,
                    'identifier': 'newer'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # verify
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.mock_package_delete_action.assert_not_called()
        self.assertEqual(self.mock_query.call_count, 1)
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_has_guid_and_newer(self, mock_harvest_object,
                                                         mock_get_harvest_config):
        '''Tests if remote dataset, which has guid and is newer, is accepted'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.remote_modified,
                    'identifier': 'with-guid',
                    'guid': '1234567890'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # verify
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.assertEqual(self.mock_package_delete_action.call_count, 1)
        self.mock_package_delete_action.assert_called_with(
            TestHarvestUtils._mock_api_context(), {'id': 'with-guid-local'})
        self.assertEqual(self.mock_query.call_count, 1)
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_with_empty_identifier(self, mock_harvest_object,
                                                            mock_get_harvest_config):
        '''Tests if remote dataset with empty field identifier is accepted'''

        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.remote_modified,
                    'identifier': ''
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # verify
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.mock_get_action.assert_not_called()
        self.mock_package_delete_action.assert_not_called()
        self.mock_query.assert_not_called()
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_with_multiple_duplicates_one_local_is_newer(self,
                                                                                  mock_harvest_object,
                                                                                  mock_get_harvest_config):
        '''Tests if remote dataset, which has multiple duplicates and one local is newer,
           is rejected'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.remote_modified,
                    'identifier': 'multiple-local-newer'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # verify
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.mock_package_delete_action.assert_called_once_with(
            TestHarvestUtils._mock_api_context(), {'id': 'two-local'})
        self.assertEqual(self.mock_query.call_count, 1)
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_remote_without_timestamp_local_has_none(self,
                                                                       mock_harvest_object,
                                                                       mock_get_harvest_config):
        '''Tests if remote dataset without timestamp and local has none is rejected'''
        # prepare
        remote_dataset = json.dumps({
                'extras': {
                    'identifier': 'multiple-without-modified-date'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        # verify
        result = HarvestUtils.handle_duplicates(harvest_object)

        # verify
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.mock_package_delete_action.assert_called_once_with(
            TestHarvestUtils._mock_api_context(), {'id': 'two-local'})
        self.assertEqual(self.mock_query.call_count, 1)
        mock_get_harvest_config.assert_not_called()

    def test_handle_duplicates_priority_check(self, mock_harvest_object, mock_get_harvest_config):
        '''Priority check Tests'''
        # Prepare both datasets got the same timestamp - same priority
        local_harvest_source = DummySource('DummyHarvester', '{"priority": 0 }')
        mock_get_harvest_config.return_value = local_harvest_source

        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.local_modified,
                    'identifier': 'hasone',
                    'metadata_harvested_portal': 'harvest2'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)

        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        mock_get_harvest_config.assert_called_once_with('source_id1')

        # Prepare both datasets got the same timestamp - local priority is higher - reject it
        self.mock_query.reset_mock()
        self.mock_get_action.reset_mock()
        self.mock_package_delete_action.reset_mock()
        mock_get_harvest_config.reset_mock()

        local_harvest_source = DummySource('DummyHarvester', '{"priority": 10 }')
        mock_get_harvest_config.return_value = local_harvest_source

        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.local_modified,
                    'identifier': 'hasone',
                    'metadata_harvested_portal': 'harvest2'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)

        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        mock_get_harvest_config.assert_called_once_with('source_id1')

        # Prepare both datasets got the same timestamp - remote priority is higher - accept it
        self.mock_query.reset_mock()
        self.mock_get_action.reset_mock()
        self.mock_package_delete_action.reset_mock()
        mock_get_harvest_config.reset_mock()

        local_harvest_source = DummySource('DummyHarvester', '{"priority": -10 }')
        mock_get_harvest_config.return_value = local_harvest_source

        remote_dataset = json.dumps({
                'extras': {
                    'modified': self.local_modified,
                    'identifier': 'hasone',
                    'metadata_harvested_portal': 'harvest2'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=self.remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)

        self.assertTrue(result, "Dataset should be accepted as update.")
        self.assertEqual(self.mock_get_action.call_count, 2)
        self.mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        mock_get_harvest_config.assert_called_once_with('source_id1')
