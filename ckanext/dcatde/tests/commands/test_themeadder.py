# -*- coding: utf8 -*-

import unittest

from ckanext.dcatde.commands.themeadder import ThemeAdder as ThemeAdderCommand
from ckanext.dcatde.commands.click.themeadder import migrate_user_permissions
import ckanext.dcatde.tests.commands.common_helpers as helpers
from mock import patch, Mock


def mock_jsonload(_):
    """Returns a dict containing the groups like json.loads would do
    with a file"""
    return {
        'agri': 'Landwirtschaft, Fischerei, Forstwirtschaft und Nahrungsmittel',
        'econ': 'Wirtschaft und Finanzen',
        'educ': 'Bildung, Kultur und Sport',
        'ener': 'Energie',
        'envi': 'Umwelt',
        'heal': 'Gesundheit',
        'intr': 'Internationale Themen',
        'just': 'Justiz, Rechtssystem und öffentliche Sicherheit',
        'soci': 'Bevölkerung und Gesellschaft',
        'gove': 'Regierung und öffentlicher Sektor',
        'regi': 'Regionen und Städte',
        'tech': 'Wissenschaft und Technologie',
        'tran': 'Verkehr'
    }


def mock_local_ckan_api(grplist):
    """Mocks action.group_list() call of LocalCKAN() to return
    the given dict"""
    action_mock = Mock()
    action_mock.group_list.return_value = grplist
    api_mock = Mock()
    api_mock.action = action_mock
    return api_mock


class GetActionHelperThemeAdder(helpers.GetActionHelper):
    """Helper class to mock toolkit.get_action"""

    def __init__(self, group_helper, group_permission_helper):
        super(GetActionHelperThemeAdder, self).__init__()
        self.group_helper = group_helper
        self.group_permission_helper = group_permission_helper

        self.return_val_actions = {
            'get_site_user': {'name': 'admin'},
            'group_list': self.group_permission_helper.group_list_val()
        }

        self.side_effect_actions = {
            'group_purge': self.group_helper.group_purge,
            'group_create': self.group_helper.group_create,
            'group_member_create': self.group_permission_helper.group_member_create
        }

        self.build_mocks()


class GroupHelper(object):
    """Helper class to keep track of added and purged groups"""

    def __init__(self):
        self.names = []
        self.purged = []

    def group_create(self, context, gdict):
        self.names.append(gdict['name'])
        return []

    def group_purge(self, context, gdict):
        self.purged.append(gdict['name'])
        return []

    def get_groups(self):
        return self.names

    def get_purged(self):
        return self.purged


class GroupPermissionHelper:
    """Keeps track of changes to group permissions."""

    def __init__(self):
        self.calls = []

    def group_member_create(self, context, gdict):
        self.calls.append(gdict)

    @staticmethod
    def group_list_val():
        return [{
            "name": "old1",
            "users": [
                {
                    "id": "userid1",
                    "name": "user1",
                    "capacity": "member"
                },
                {
                    "id": "userid2",
                    "name": "user2",
                    "capacity": "admin"
                }
            ]
        },
        {
            "name": "old2",
            "users": [
                {
                    "id": "userid2",
                    "name": "user2",
                    "capacity": "member"
                },
                {
                    "id": "userid3",
                    "name": "user3",
                    "capacity": "editor"
                }
            ]
        }]

    def get_calls(self):
        return self.calls


@patch("ckan.plugins.toolkit.get_action")
@patch("ckanext.dcatde.commands.cli.CkanCommand._load_config")
@patch("ckanapi.LocalCKAN")
@patch("json.loads", mock_jsonload)
class TestThemeAdderCommand(unittest.TestCase):
    """Test class for the dcatde default theme adder"""

    def setUp(self):
        self.cmd = ThemeAdderCommand(name='ThemeAdderTest')

    def _assert_groups_valid(self, helper):
        """Checks if the groups for which the GroupHelper was called are present
        in the test data set."""
        for group in helper.get_groups():
            self.assertTrue(group in mock_jsonload('').keys(),
                            "Nonexisting group was added: " + group)

        for group in helper.get_purged():
            self.assertTrue(group in mock_jsonload('').keys(),
                            "purge was called for nonexisting group: " + group)

    def _assert_purge_and_add(self, helper, group_key):
        """Asserts that group_purge and group_add have been called
        for the key"""
        self.assertTrue(group_key in helper.get_groups(),
                        "Group not added: " + group_key)
        self.assertTrue(group_key in helper.get_purged(),
                        "Purge not called for: " + group_key)

    def test_add_nonexisting_groups(self, mock_ckanapi_LocalCKAN,
                                    mock_super_load_config,
                                    mock_getaction):
        """Adds all groups assuming they are not present"""
        # prepare
        self.cmd.args = []
        mock_ckanapi_LocalCKAN.return_value = mock_local_ckan_api({})

        helper = GroupHelper()
        permissionhelper = GroupPermissionHelper()
        action_hlp = GetActionHelperThemeAdder(helper, permissionhelper)

        mock_getaction.side_effect = action_hlp.mock_get_action

        # execute
        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # check if gropus were added
        self._assert_groups_valid(helper)

        for group_key in mock_jsonload(''):
            self._assert_purge_and_add(helper, group_key)

    def test_add_with_existing_groups(self, mock_ckanapi_LocalCKAN,
                                      mock_super_load_config,
                                      mock_getaction):
        """Adds all groups assuming some are present"""
        # prepare
        self.cmd.args = []
        # use a string map for simulating present groups, as only keys are
        # relevant
        testmap = {'agri': '', 'intr': ''}
        mock_ckanapi_LocalCKAN.return_value = mock_local_ckan_api(testmap)

        helper = GroupHelper()
        permissionhelper = GroupPermissionHelper()
        action_hlp = GetActionHelperThemeAdder(helper, permissionhelper)

        mock_getaction.side_effect = action_hlp.mock_get_action

        # execute
        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # check if nonpresent gropus were added
        self._assert_groups_valid(helper)

        for group_key in filter(lambda k: k not in testmap, mock_jsonload('')):
            self._assert_purge_and_add(helper, group_key)

        # and make sure the present groups were not touched
        for group_key in testmap:
            self.assertFalse(group_key in helper.get_groups(),
                             "Group added although present: " + group_key)
            self.assertFalse(group_key in helper.get_purged(),
                             "Group purged although present: " + group_key)

    def test_add_all_groups_present(self, mock_ckanapi_LocalCKAN,
                                    mock_super_load_config,
                                    mock_getaction):
        """Calls the adder assuming all groups are already existing"""
        # prepare
        self.cmd.args = []
        # As only keys are relevant, we can use our test data as group list
        mock_ckanapi_LocalCKAN.return_value = mock_local_ckan_api(
            mock_jsonload(''))

        helper = GroupHelper()
        permissionhelper = GroupPermissionHelper()
        action_hlp = GetActionHelperThemeAdder(helper, permissionhelper)

        mock_getaction.side_effect = action_hlp.mock_get_action

        # execute
        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # ensure no groups were added
        self.assertEqual(len(helper.get_groups()), 0,
                         "Groups were added although present")
        self.assertEqual(len(helper.get_purged()), 0,
                         "Groups were purged although present")

    def test_group_permission_migration(self, mock_ckanapi_LocalCKAN,
                                        mock_super_load_config,
                                        mock_getaction):

        helper = GroupHelper()
        permissionhelper = GroupPermissionHelper()
        action_hlp = GetActionHelperThemeAdder(helper, permissionhelper)

        mock_getaction.side_effect = action_hlp.mock_get_action

        migrate_user_permissions(["old1", "old2"], ["new1", "new2"])

        calls = permissionhelper.get_calls()
        self.assertListEqual(calls, [
            {
                "id": "new1",
                "username": "user1",
                "role": "member"
            }, {
                "id": "new2",
                "username": "user1",
                "role": "member"
            }, {
                "id": "new1",
                "username": "user2",
                "role": "admin"
            }, {
                "id": "new2",
                "username": "user2",
                "role": "admin"
            }, {
                "id": "new1",
                "username": "user3",
                "role": "editor"
            }, {
                "id": "new2",
                "username": "user3",
                "role": "editor"
            }
        ], "calls do not match")
