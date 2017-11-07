#!/usr/bin/python
# -*- coding: utf8 -*-

import unittest

from ckanext.dcatde.commands.migration import DCATdeMigrateCommand
import ckanext.dcatde.tests.commands.common_helpers as helpers
from mock import patch, call


PYLONS_TEST_CFG = {
    'ckanext.dcatde.urls.license_mapping': 'licenses.json',
    'ckanext.dcatde.urls.category_mapping': 'categories.json'
}


def mock_load_json_mapping(filename, _):
    '''mock for util.load_json_mapping which returns dummy data'''
    if filename == 'licenses.json':
        return {
            'list': [{
                'URI': 'new-id',
                'OGDLizenzcode': 'id1'
            }]
        }
    elif filename == 'categories.json':
        return {'needed': 'new'}
    else:
        return None


class GetActionHelperMigration(helpers.GetActionHelper):
    '''toolkit.get_action mocks for the migration command'''
    def __init__(self):
        super(GetActionHelperMigration, self).__init__()
        self.updated_datasets = {}
        self.shown_ids = []

        self.return_val_actions = {
            'group_list': {'new': ''},
            'package_list': ['pkg1', 'pkg2']
        }

        self.side_effect_actions = {
            'package_show': self.mock_pkg_show,
            'package_update': self.mock_pkg_update
        }

        self.build_mocks()

    def mock_pkg_show(self, _, data):
        if not data or 'id' not in data:
            return None

        ds_id = data['id']
        dataset = {'id': ds_id}
        self.shown_ids.append(ds_id)

        if ds_id == 'pkg1':
            dataset['groups'] = [{'name': 'needed'}]
            dataset['license_id'] = 'id1'
            dataset['resources'] = {}
            dataset['type'] = 'datensatz'
        elif ds_id == 'pkg2':
            dataset['type'] = 'harvest'

        return dataset

    def mock_pkg_update(self, _, dataset):
        self.updated_datasets[dataset['id']] = dataset


@patch("ckanext.dcatde.commands.migration.or_", autospec=True)
@patch("ckanext.dcatde.commands.migration.model", autospec=True)
@patch("ckan.plugins.toolkit.get_action")
@patch("ckan.lib.cli.CkanCommand._load_config")
@patch("ckanext.dcatde.migration.util.load_json_mapping",
       mock_load_json_mapping)
@patch.dict("pylons.config", PYLONS_TEST_CFG)
class TestMigration(unittest.TestCase):
    '''Tests the CKAN DCATde migration command.'''

    def setUp(self):
        self.cmd = DCATdeMigrateCommand(name='ThemeAdderTest')
        self.cmd.args = []

    def _assert_group_list_once(self, action_hlp):
        '''Asserts that the mocked group_list on the action helper
        instance was called once'''
        grp_mock = action_hlp.get_mock_for('group_list')
        self.assertEquals(grp_mock.call_count, 1, 'group_list not called once')

    def _assert_package_show(self, action_hlp):
        '''Asserts that package_show was called for the IDs in package_list'''
        show_mock = action_hlp.get_mock_for('package_show')
        self.assertEquals(show_mock.call_count, 2,
                          'Expecting 2 calls to package_show')
        self.assertTrue('pkg1' in action_hlp.shown_ids)
        self.assertTrue('pkg2' in action_hlp.shown_ids)

    def _assert_db_updated(self, mock_model, mock_sa_or):
        '''Checks if the Package schema's type attributes were updated'''
        # only look if a subset (i.e. filter was used) of the package schema
        # gets dataset as type
        mock_model.Session.query.assert_called_with(mock_model.Package)
        self.assertEquals(mock_model.Session.query.call_count, 2,
                          'Expecting 2 calls to Session.query')
        self.assertTrue(
            call.query().filter(mock_sa_or).update({'type': u'dataset'}) in
            mock_model.Session.mock_calls
        )
        self.assertTrue(
            call.query().filter(mock_sa_or).filter(True) in
            mock_model.Session.mock_calls
        )
        mock_model.repo.commit.assert_any_call()

    def _assert_db_not_updated(self, mock_model, mock_sa_or):
        '''Asserts that the datbase logic was not called'''
        mock_model.Session.query.assert_called_once_with(mock_model.Package)
        self.assertTrue(
            call.query().filter(mock_sa_or).filter(True) in
            mock_model.Session.mock_calls
        )
        mock_model.repo.commit.assert_not_called()

    def _assert_db_not_called(self, mock_model):
        '''Asserts that the datbase logic was not called'''
        mock_model.Session.query.assert_not_called()
        mock_model.repo.commit.assert_not_called()

    def test_without_groups(self, mock_super_load_config,
                            mock_get_action, mock_model, mock_sa_or):
        '''Calls the migration command assuming no new groups were added.'''
        action_hlp = GetActionHelperMigration()
        # override the group mock
        action_hlp.return_val_actions['group_list'] = {}
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = []
        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # only the group_list call may have been obtained, and no further
        # actions were excuted, as groups were not present
        mock_get_action.assert_called_once_with('group_list')
        self._assert_group_list_once(action_hlp)

        # no DB logic may have been called
        self._assert_db_not_called(mock_model)

    def test_with_groups(self, mock_super_load_config,
                         mock_get_action, mock_model, mock_sa_or):
        '''Calls the migration command assuming new groups were added.'''
        action_hlp = GetActionHelperMigration()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = []
        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # assert that the needed methods were obtained in the expected
        # order. Update gets obtained for each dataset, but we skip one
        # of the datasets due to type harvest.
        mock_get_action.assert_has_calls([call('group_list'),
                                         call('package_list'),
                                         call('package_show'),
                                         call('package_update')])
        self.assertEqual(mock_get_action.call_count, 4)
        self._assert_group_list_once(action_hlp)
        # only the datasets from package_list were shown
        self._assert_package_show(action_hlp)

        # the harvester was not migrated
        self.assertTrue('pkg2' not in action_hlp.updated_datasets,
                        'Harvester not skipped')

        # the specified license mapping was used
        self.assertEqual(action_hlp.updated_datasets['pkg1']['license_id'],
                         'new-id')

        # the specified group mapping was used
        groups = action_hlp.updated_datasets['pkg1']['groups']
        self.assertEquals(len(groups), 1, 'Not one group')
        self.assertDictContainsSubset({'name': 'new'}, groups[0])

        # database was changed
        self._assert_db_updated(mock_model, mock_sa_or)

    def test_dry_run(self, mock_super_load_config,
                     mock_get_action, mock_model, mock_sa_or):
        '''Calls the migration command with the dry-run flag.'''
        action_hlp = GetActionHelperMigration()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = ['dry-run']
        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # assert that the needed methods were obtained in the expected
        # order.
        # package_update may not have been obtained due to the flag.
        mock_get_action.assert_has_calls([call('group_list'),
                                         call('package_list'),
                                         call('package_show')])
        self.assertEqual(mock_get_action.call_count, 3)
        self._assert_group_list_once(action_hlp)

        # only the datasets from package_list were shown
        self._assert_package_show(action_hlp)

        # no DB logic may have been called
        self._assert_db_not_updated(mock_model, mock_sa_or)
