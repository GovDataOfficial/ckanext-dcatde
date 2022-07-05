#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import unittest

from ckanext.dcatde.harvesters.harvest_utils import HarvestUtils
from ckanext.harvest.model import HarvestObject
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

    @patch('ckanext.dcatde.harvesters.harvest_utils.HarvestObject')
    @patch('ckanext.dcatde.harvesters.harvest_utils.model')
    @patch('ckan.plugins.toolkit.get_action')
    def test_handle_duplicates(self, mock_get_action, mock_model, mock_harvest_object):
        local_modified = '2017-08-14T10:00:00.000'
        remote_modified = '2017-08-15T10:00:00+02:00'
        local_newer_modified = '2017-08-17T10:00:00.000'

        mock_package_delete_action = Mock("package_delete")

        mock_query = Mock(name='query')
        mock_update_harvest_obj = Mock(name='update-harvest-obj')
        mock_query.side_effect =  mock_update_harvest_obj
        mock_model.Session.query = mock_query

        remote_source = json.dumps({
            'source': {
                'title': 'DummyHarvester'
            }
        })

        def mock_action_methods(action):
            if action == 'package_search':
                return package_search_action
            if action == 'package_delete':
                return mock_package_delete_action

        # Return local dataset
        def package_search_action(context, data_dict):
            if data_dict["q"] == 'identifier:"hasone"':
                return {
                           'count': 1,
                           'results': [{
                               'id': 'hasone-local',
                               'name': 'hasone-name',
                               'extras': [{'key': 'modified', 'value': local_modified},
                                          {'key': 'metadata_harvested_portal', 'value': 'harvest1'}]
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
                               'extras': [{'key': 'modified', 'value': local_modified}]
                           }, {
                               'id': 'two-local',
                               'name': 'two-name',
                               'extras': [{'key': 'modified', 'value': local_modified}]
                           }]
                       }
            elif data_dict["q"] == 'identifier:"newer"':
                return {
                           'count': 1,
                           'results': [{
                               'id': 'newer-local',
                               'name': 'newer-name',
                               'extras': [{'key': 'modified', 'value': local_newer_modified}]
                           }]
                       }
            elif data_dict["q"] == 'identifier:"with-guid"' and data_dict["fq"] == '-guid:"1234567890"':
                return {
                            'count': 1,
                            'results': [{
                                'id': 'with-guid-local',
                                'name': 'with-guid-name',
                                'identifier': 'with-guid',
                                'extras': [{'key': 'modified', 'value': local_modified}]
                            }]
                       }
            elif data_dict["q"] == 'identifier:"multiple-local-newer"':
                return {
                           'count': 2,
                           'results': [{
                               'id': 'one-local',
                               'name': 'one-name',
                               'extras': [{'key': 'modified', 'value': local_newer_modified}]
                           }, {
                               'id': 'two-local',
                               'name': 'two-name',
                               'extras': [{'key': 'modified', 'value': local_modified}]
                           }]
                       }
            elif data_dict["q"] == 'identifier:"multiple-without-modified-date"':
                return {
                           'count': 2,
                           'results': [{
                               'id': 'one-local',
                               'name': 'one-name',
                               'metadata_modified': local_newer_modified,
                               'extras': []
                           }, {
                               'id': 'two-local',
                               'name': 'two-name',
                               'metadata_modified': local_modified,
                               'extras': []
                           }]
                       }
            else:
                raise AssertionError('Unexpected query!')

        mock_get_action.side_effect = mock_action_methods

        # Prepare remote dataset, which is newer
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'hasone'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)
        mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.assertEqual(mock_package_delete_action.call_count, 1)
        mock_package_delete_action.assert_called_with(
            TestHarvestUtils._mock_api_context(), {'id': 'hasone-local'})
        self.assertEqual(mock_query.call_count, 1)

        # Prepare remote dataset, which has multiple duplicates
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'multiple'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)
        mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.assertEqual(mock_package_delete_action.call_count, 2)
        mock_package_delete_action.assert_has_calls([
            call(TestHarvestUtils._mock_api_context(), {'id': 'one-local'}),
            call(TestHarvestUtils._mock_api_context(), {'id': 'two-local'})],
            any_order=True)
        self.assertEqual(mock_query.call_count, 1)

        # Prepare remote dataset, which is newer - and no local dataset, so it is to be accepted
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'nodata'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 1)
        mock_get_action.assert_has_calls([call("package_search")])
        mock_package_delete_action.assert_not_called()
        mock_query.assert_not_called()

        # Prepare remote dataset without ID - accept
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertTrue(result, "Dataset was not accepted as update.")
        mock_get_action.assert_not_called()
        mock_package_delete_action.assert_not_called()
        mock_query.assert_not_called()

        # Prepare remote dataset without timestamp, but local has older one - reject
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'identifier': 'hasone'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)
        mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        mock_package_delete_action.assert_not_called()
        self.assertEqual(mock_query.call_count, 1)

        # Prepare remote dataset without timestamp, but same harvester - reject, because accepted no more
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'identifier': 'hasone',
                    'metadata_harvested_portal': 'harvest1'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertFalse(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)
        mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        mock_package_delete_action.assert_not_called()
        self.assertEqual(mock_query.call_count, 1)

        # Prepare remote dataset - and local dataset is newer, so reject this
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'newer'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)
        mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        mock_package_delete_action.assert_not_called()
        self.assertEqual(mock_query.call_count, 1)

        # Prepare remote dataset has guid and remote dataset is newer, so accept it
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'with-guid',
                    'guid': '1234567890'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertTrue(result, "Dataset was not accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)
        mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        self.assertEqual(mock_package_delete_action.call_count, 1)
        mock_package_delete_action.assert_called_with(
            TestHarvestUtils._mock_api_context(), {'id': 'with-guid-local'})
        self.assertEqual(mock_query.call_count, 1)

        # Prepare remote dataset has an empty field identifier, so accept it
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': ''
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertTrue(result, "Dataset was not accepted as update.")
        mock_get_action.assert_not_called()
        mock_package_delete_action.assert_not_called()
        mock_query.assert_not_called()

        # Prepare remote dataset, which has multiple duplicates. One local is newer - reject
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'modified': remote_modified,
                    'identifier': 'multiple-local-newer'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)
        mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        mock_package_delete_action.assert_called_once_with(
            TestHarvestUtils._mock_api_context(), {'id': 'two-local'})
        self.assertEqual(mock_query.call_count, 1)

        # Prepare remote dataset without timestamp and local has none - reject - keep last modified local
        mock_query.reset_mock()
        mock_get_action.reset_mock()
        mock_package_delete_action.reset_mock()
        remote_dataset = json.dumps({
                'extras': {
                    'identifier': 'multiple-without-modified-date'
                }
            })
        harvest_object = HarvestObject(content=remote_dataset, source=remote_source)

        result = HarvestUtils.handle_duplicates(harvest_object)
        self.assertFalse(result, "Dataset should not be accepted as update.")
        self.assertEqual(mock_get_action.call_count, 2)
        mock_get_action.assert_has_calls([call("package_search"), call("package_delete")])
        mock_package_delete_action.assert_called_once_with(
            TestHarvestUtils._mock_api_context(), {'id': 'two-local'})
        self.assertEqual(mock_query.call_count, 1)

    def test_compare_metadata_modified(self):
        # Remote date is newer than local date
        date_string_local = '2017-08-14T10:00:00.000'
        date_string_remote = '2017-08-15T10:00:00.000'
        result = HarvestUtils.compare_metadata_modified(date_string_remote, date_string_local)
        self.assertTrue(result, "Expected remote date is newer!")

        # Remote date is equal to local date
        date_string_local = '2017-08-14T10:00:00.000'
        date_string_remote = '2017-08-14T10:00:00.000'
        result = HarvestUtils.compare_metadata_modified(date_string_remote, date_string_local)
        self.assertFalse(result, "Expected remote date is equal!")

        # Remote date is older than local date
        date_string_local = '2017-08-15T10:00:00.000'
        date_string_remote = '2017-08-14T10:00:00.000'
        result = HarvestUtils.compare_metadata_modified(date_string_remote, date_string_local)
        self.assertFalse(result, "Expected remote date is older!")

        # Remote date with time zone and local date without time zone
        date_string_local = '2017-08-14T10:00:00.000'
        date_string_remote = '2017-08-15T10:00:00+01:00'
        result = HarvestUtils.compare_metadata_modified(date_string_remote, date_string_local)
        self.assertTrue(result, "Expected remote date is newer!")

        # Both, remote date and local date with time zone
        date_string_local = '2017-08-14T10:00:00+02:00'
        date_string_remote = '2017-08-14T10:00:00+01:00'
        result = HarvestUtils.compare_metadata_modified(date_string_remote, date_string_local)
        self.assertTrue(result, "Expected remote date is newer!")

        # Remote date is equal to local date (both with time zone)
        date_string_local = '2017-08-14T10:00:00+01:00'
        date_string_remote = '2017-08-14T10:00:00+01:00'
        result = HarvestUtils.compare_metadata_modified(date_string_remote, date_string_local)
        self.assertFalse(result, "Expected remote date is equal!")
