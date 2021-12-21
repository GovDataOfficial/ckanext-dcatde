#!/usr/bin/python
# -*- coding: utf8 -*-

import unittest

from ckanext.dcatde.commands.migration import DCATdeMigrateCommand
import ckanext.dcatde.tests.commands.common_helpers as helpers
from mock import patch, call, Mock, MagicMock

PYLONS_TEST_CFG = {
    'ckanext.dcatde.urls.license_mapping': 'licenses.json',
    'ckanext.dcatde.urls.category_mapping': 'categories.json'
}

CONTRIBUTOR_ID_NEW = "http://dcat-ap.de/def/contributors/bundesministeriumFuerErnaehrungUndLandwirtschaft"

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
            'package_list': ['pkg1', 'pkg2'],
            'organization_list': [{'id': '123'}],
            'get_site_user': {'name': 'admin'}
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
        dataset = {'id': ds_id, 'type': 'datensatz', 'title': 'titel', 'organization': {'id': '123'}}
        self.shown_ids.append(ds_id)

        if ds_id == 'pkg1':
            dataset['groups'] = [{'name': 'needed'}]
            dataset['license_id'] = 'id1'
            dataset['resources'] = {}
        elif ds_id == 'pkg2':
            dataset['type'] = 'harvest'
        elif ds_id == 'pkg3':
            dataset['extras'] = [{'key': 'alternate_identifier', 'value': 'adms-id'}]
        elif ds_id == 'pkg4':
            dataset['extras'] = [{'key': 'alternate_identifier', 'value': 'adms-id'},
                                 {'key': 'identifier', 'value': 'existing-id'}]
        elif ds_id == 'pkg5':
            dataset['extras'] = [{'value': '["test_ds_contrib_id"]', 'key': 'contributorID'}]
        elif ds_id == 'pkg6':
            dataset['extras'] = []
        elif ds_id == 'pkg7':
            dataset['extras'] = [{'value': '["test_org_contrib_id"]', 'key': 'contributorID'}]
        elif ds_id == 'pkg8':
            dataset['extras'] = [{'value': 'test_ds_contrib_id', 'key': 'contributorID'}]
        elif ds_id == 'pkg9':
            dataset['extras'] = [{'value': '["http://dcat-ap.de/def/contributors/bundesanstaltFuerLandwirtschaftUndErnaehrung","test_org_contrib_id","test_org_contrib_id"]', 'key': 'contributorID'}]

        return dataset

    def mock_pkg_update(self, _, dataset):
        self.updated_datasets[dataset['id']] = dataset

@patch("ckanext.dcatde.commands.click.migration.gather_dataset_ids")
@patch("ckanext.dcatde.commands.click.migration.or_", autospec=True)
@patch("ckanext.dcatde.commands.click.migration.model", autospec=True)
@patch("ckan.plugins.toolkit.get_action")
@patch("ckanext.dcatde.commands.cli.CkanCommand._load_config")
@patch("ckanext.dcatde.migration.util.load_json_mapping",
       mock_load_json_mapping)
@patch.dict("ckan.plugins.toolkit.config", PYLONS_TEST_CFG)
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

    def _mock_model_query_filter(self, mock_model, pkg_id='pkg3'):
        """
        Mocks calls to query().filter()[.filter()...] such that they return a dataset ID
        """
        mock_package = Mock(package_id=pkg_id)
        mock_filter = MagicMock()
        # make filter() return itself to allow variable length chains, and use the list iterator for it
        mock_filter.filter.return_value = mock_filter
        mock_filter.__iter__.return_value = iter([mock_package])
        mock_query = Mock()
        mock_query.return_value = mock_filter
        mock_model.Session.query = mock_query

    def test_without_groups(self, mock_super_load_config,
                            mock_get_action, mock_model, mock_sa_or, _):
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
                         mock_get_action, mock_model, mock_sa_or, _):
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
        self.assertEqual(mock_get_action.call_count, 5)
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
                     mock_get_action, mock_model, mock_sa_or, __):
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

    def test_adms_id(self, mock_super_load_config,
                     mock_get_action, mock_model, _, __):
        """
        Calls the migration command with adms-id-migrate flag.
        """
        action_hlp = GetActionHelperMigration()
        mock_get_action.side_effect = action_hlp.mock_get_action

        # mock the database query to return the test package ID
        self._mock_model_query_filter(mock_model)

        self.cmd.args = ['adms-id-migrate']
        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # assert that the needed methods were obtained in the expected
        # order. Update gets obtained for each dataset, but we only use one dataset anyway.
        mock_get_action.assert_has_calls([call('package_show'),
                                          call('package_update'),
                                          call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 3)

        # check if the extras field contains exactly one entry with the new key
        self.assertEqual(action_hlp.updated_datasets['pkg3']['extras'],
                         [{'key': 'identifier', 'value': 'adms-id'}])

    def _check_adms_id_no_save(self, mock_super_load_config, mock_get_action):
        """
        Helper to run the command and perform checks for test_adms_id_dry
        """
        # run
        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # assert that only the package was shown and no update call was made.
        mock_get_action.assert_has_calls([call('package_show')])
        self.assertEqual(mock_get_action.call_count, 1)

    def test_adms_id_dry(self, mock_super_load_config,
                         mock_get_action, mock_model, _, __):
        """
        Calls the migration command with adms-id-migrate and dry-run flags.
        """
        action_hlp = GetActionHelperMigration()
        mock_get_action.side_effect = action_hlp.mock_get_action

        # mock the database query to return the test package ID
        self._mock_model_query_filter(mock_model)

        # test both arrangements of the arguments
        self.cmd.args = ['adms-id-migrate', 'dry-run']
        self._check_adms_id_no_save(mock_super_load_config, mock_get_action)

        mock_super_load_config.reset_mock()
        mock_get_action.reset_mock()
        mock_model.reset_mock()
        self.cmd.args = ['dry-run', 'adms-id-migrate']
        self._check_adms_id_no_save(mock_super_load_config, mock_get_action)

    def test_adms_id_with_dct_id(self, mock_super_load_config,
                                 mock_get_action, mock_model, _, __):
        """
        Calls the migration command with adms-id-migrate. Assumes there is a dataset which
        already has an extras.identifier.
        """
        action_hlp = GetActionHelperMigration()
        mock_get_action.side_effect = action_hlp.mock_get_action

        # mock the database query to return the test package ID with existing extras.identifier
        self._mock_model_query_filter(mock_model, pkg_id='pkg4')

        # run the command and assert nothing was saved
        self.cmd.args = ['adms-id-migrate']
        self._check_adms_id_no_save(mock_super_load_config, mock_get_action)

    @patch("ckanext.dcatde.commands.click.migration.update_dataset")
    def test_contributor_id_add_id_to_existing_field(self, mock_update_dataset, mock_super_load_config, mock_get_action,
                                                     mock_model, mock_sa_or, mock_gather_ids):
        """ Tests if the organization contributor-id has been added to the existing contributor-ids """
        pkg_id = 'pkg5'
        organization_id = '123'

        mock_gather_ids.return_value = {pkg_id: organization_id}

        action_hlp = GetActionHelperMigration()
        action_hlp.return_val_actions['organization_list'] = [{'id': organization_id,
                                                               'extras': [{'value': '["test_org_contrib_id"]',
                                                                           'key': 'contributorID'}]}]
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = ['contributor-id-migrate']
        self.cmd.command()

        mock_super_load_config.assert_called_once_with()
        mock_get_action.assert_has_calls([call('package_show')])
        self.assertEqual(mock_get_action.call_count, 2)
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with()
        mock_update_dataset.assert_called_once_with({'organization': {'id': organization_id}, 'extras': [{
            'key': 'contributorID', 'value': '["test_ds_contrib_id", "test_org_contrib_id"]'}],
            'type': 'datensatz', 'id': pkg_id, 'title': 'titel'}, dry_run=False)

    @patch("ckanext.dcatde.commands.click.migration.update_dataset")
    def test_contributor_id_create_id_field(self, mock_update_dataset, mock_super_load_config, mock_get_action, mock_model,
                                            mock_sa_or, mock_gather_ids):
        """ Tests if a new contributor-id-field has been created in the dataset"""
        pkg_id = 'pkg6'
        organization_id = '123'

        mock_gather_ids.return_value = {pkg_id: organization_id}

        action_hlp = GetActionHelperMigration()
        action_hlp.return_val_actions['organization_list'] = [{'id': organization_id,
                                                               'extras': [{'value': '["test_org_contrib_id"]',
                                                                           'key': 'contributorID'}]}]
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = ['contributor-id-migrate']
        self.cmd.command()

        mock_super_load_config.assert_called_once_with()
        mock_get_action.assert_has_calls([call('package_show')])
        self.assertEqual(mock_get_action.call_count, 2)
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with()
        mock_update_dataset.assert_called_once_with({'organization': {'id': organization_id}, 'extras': [{
            'key': 'contributorID', 'value': '["test_org_contrib_id"]'}],
            'type': 'datensatz', 'id': pkg_id, 'title': 'titel'}, dry_run=False)

    @patch("ckanext.dcatde.commands.click.migration.update_dataset")
    def test_contributor_id_already_exists(self, mock_update_dataset, mock_super_load_config, mock_get_action, mock_model,
                                           mock_sa_or, mock_gather_ids):
        """
        Tests if the organization contributor-id will not be added if it already exists for the dataset
        """
        pkg_id = 'pkg7'
        organization_id = '123'

        mock_gather_ids.return_value = {pkg_id: organization_id}

        action_hlp = GetActionHelperMigration()
        action_hlp.return_val_actions['organization_list'] = [{
            'id': organization_id, 'extras': [{'value': '["test_org_contrib_id"]', 'key': 'contributorID'}]}]
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = ['contributor-id-migrate']
        self.cmd.command()

        mock_super_load_config.assert_called_once_with()
        mock_get_action.assert_has_calls([call('package_show')])
        self.assertEqual(mock_get_action.call_count, 2)
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with()
        mock_update_dataset.assert_not_called()

    @patch("ckanext.dcatde.commands.click.migration.update_dataset")
    def test_contributor_id_single_values_add_to_field(self, mock_update_dataset, mock_super_load_config, mock_get_action,
                                                     mock_model, mock_sa_or, mock_gather_ids):
        """ Tests if the organization contributor-id has been added to the existing contributor-ids.
         The contributor-IDs are not lists but Strings """
        pkg_id = 'pkg8'
        organization_id = '123'

        mock_gather_ids.return_value = {pkg_id: organization_id}

        action_hlp = GetActionHelperMigration()
        action_hlp.return_val_actions['organization_list'] = [{'id': organization_id,
                                                               'extras': [{'value': 'test_org_contrib_id',
                                                                           'key': 'contributorID'}]}]
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = ['contributor-id-migrate']
        self.cmd.command()

        mock_super_load_config.assert_called_once_with()
        mock_get_action.assert_has_calls([call('package_show')])
        self.assertEqual(mock_get_action.call_count, 2)
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with()
        mock_update_dataset.assert_called_once_with({'organization': {'id': organization_id}, 'extras': [{
            'key': 'contributorID', 'value': '["test_ds_contrib_id", "test_org_contrib_id"]'}],
            'type': 'datensatz', 'id': pkg_id, 'title': 'titel'}, dry_run=False)

    @patch("ckanext.dcatde.commands.click.migration.update_dataset")
    def test_contributor_id_no_contributorid_in_org(self, mock_update_dataset, mock_super_load_config, mock_get_action,
                                                    mock_model, mock_sa_or, mock_gather_ids):
        """ Tests if the organization has no contributor-id defined that migration is skipped for the
        datasets of the org """
        pkg_id = 'pkg5'
        organization_id = '123'

        mock_gather_ids.return_value = {pkg_id: organization_id}

        action_hlp = GetActionHelperMigration()
        # empty extras
        action_hlp.return_val_actions['organization_list'] = [{'id': organization_id, 'extras': []}]
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = ['contributor-id-migrate']
        self.cmd.command()

        mock_super_load_config.assert_called_once_with()
        mock_get_action.assert_has_calls([call('package_show')])
        self.assertEqual(mock_get_action.call_count, 2)
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with()
        mock_update_dataset.assert_not_called()

    @patch("ckanext.dcatde.commands.click.migration.update_dataset")
    def test_deprecated_contributor_id(self, mock_update_dataset, mock_super_load_config, mock_get_action, mock_model,
                                           mock_sa_or, mock_gather_ids):
        """ Tests if a deprecated contributorID is detected and replaced """
        pkg_id = 'pkg9'
        organization_id = '123'

        mock_gather_ids.return_value = {pkg_id: organization_id}

        # The contributorID in the CKAN organization was already updated before
        action_hlp = GetActionHelperMigration()
        action_hlp.return_val_actions['organization_list'] = [{
            'id': organization_id, 'extras': [{'value': '["' + CONTRIBUTOR_ID_NEW + '"]', 'key': 'contributorID'}]}]
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action

        self.cmd.args = ['contributor-id-migrate']
        self.cmd.command()

        mock_super_load_config.assert_called_once_with()
        mock_get_action.assert_has_calls([call('package_show')])
        self.assertEqual(mock_get_action.call_count, 2)
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with()
        mock_update_dataset.assert_called_once_with({'organization': {'id': organization_id}, 'extras': [{
            'key': 'contributorID', 'value': '["test_org_contrib_id", "' + CONTRIBUTOR_ID_NEW + '"]'}],
            'type': 'datensatz', 'id': pkg_id, 'title': 'titel'}, dry_run=False)
