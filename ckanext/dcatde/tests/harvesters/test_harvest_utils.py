#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import unittest

from ckanext.dcatde.harvesters.harvest_utils import HarvestUtils
from mock import call, patch, Mock, ANY


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

    @patch('ckan.plugins.toolkit.get_action')
    def test_rename_delete_dataset_with_id(self, mock_get_action):
        mock_action_methods = Mock("action-methods")
        # always return a pseudo-package from API methods
        mock_action_methods.return_value = {'id': 'test', 'name': 'package'}
        mock_get_action.return_value = mock_action_methods

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

        # finally check if the name passed to the update call was different
        self.assertTrue(
            call(TestHarvestUtils._mock_api_context(),
                 {'id': 'test', 'name': 'package'}) not in mock_action_methods.call_args_list,
            "Name was not updated")

    @patch('ckan.plugins.toolkit.get_action')
    def test_handle_duplicates(self, mock_get_action):
        local_modified = '2017-08-14T10:00:00.000'
        remote_modified = '2017-08-15T10:00:00.000'
        local_newer_modified = '2017-08-17T10:00:00.000'

        # Return local dataset
        def mock_get_action_function(context, data_dict):
            if data_dict["q"] == 'identifier:"hasone"':
                return {
                           'count': 1,
                           'results': [{
                               'extras': [{'key': 'modified', 'value': local_modified}]
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
                               'extras': [{'key': 'modified', 'value': local_modified}]
                           }, {
                               'extras': [{'key': 'modified', 'value': local_modified}]
                           }]
                       }
            elif data_dict["q"] == 'identifier:"newer"':
                return {
                           'count': 1,
                           'results': [{
                               'extras': [{'key': 'modified', 'value': local_newer_modified}]
                           }]
                       }
            elif data_dict["q"] == 'identifier:"with-guid"' and data_dict["fq"] == '-guid:"1234567890"':
                return {
                            'count': 1,
                            'results': [{
                                'identifier': 'with-guid',
                                'extras': [{'key': 'modified', 'value': local_modified}]
                            }]
                       }
            else:
                raise AssertionError('Unexpected query!')

        mock_get_action.return_value = mock_get_action_function

        # Prepare remote dataset, which is newer
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'hasone'
                }
            })

        result = HarvestUtils.handle_duplicates(remote_dataset)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 1)

        # Prepare remote dataset, which has multiple duplicates
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'multiple'
                }
            })

        result = HarvestUtils.handle_duplicates(remote_dataset)
        self.assertFalse(result, "Dataset was accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)

        # Prepare remote dataset, which is newer - and no local dataset, so it is to be accepted
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'nodata'
                }
            })

        result = HarvestUtils.handle_duplicates(remote_dataset)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 3)

        # Prepare remote dataset without ID - acccept
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                }
            })

        result = HarvestUtils.handle_duplicates(remote_dataset)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 3)

        # Prepare remote dataset without timestamp - reject
        remote_dataset = json.dumps({
                'extras': {
                    'identifier': 'hasone'
                }
            })

        result = HarvestUtils.handle_duplicates(remote_dataset)
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(mock_get_action.call_count, 4)

        # Prepare remote dataset - and local dataset is newer, so reject this
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'newer'
                }
            })

        result = HarvestUtils.handle_duplicates(remote_dataset)
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(mock_get_action.call_count, 5)

        # Prepare remote dataset has guid and remote dataset is newer, so accept it
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'with-guid',
                    'guid': '1234567890'
                }
            })

        result = HarvestUtils.handle_duplicates(remote_dataset)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 6)

        # Prepare remote dataset has an empty field identifier, so accept it
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': ''
                }
            })

        result = HarvestUtils.handle_duplicates(remote_dataset)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 6)
