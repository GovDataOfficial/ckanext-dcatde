#!/usr/bin/python
# -*- coding: utf8 -*-

import unittest

from ckanext.dcatde.commands.triplestore import Triplestore
from mock import patch, call, Mock, MagicMock


class DummyClass:
    pass


@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available", autospec=True)
@patch("ckanext.dcatde.commands.triplestore.model", autospec=True)
@patch("ckan.plugins.toolkit.get_action")
@patch("ckan.lib.cli.CkanCommand._load_config")
class TestTripleStoreCommand(unittest.TestCase):
    '''Tests the CKAN DCATde triplestore command.'''

    def setUp(self):
        self.cmd = Triplestore(name='TripleStoreTest')
        self.cmd.args = []
        self.cmd.options = DummyClass()
        self.cmd.options.dry_run = 'True'

    def tearDown(self):
        # Remove option to avoid OptionConflictError
        self.cmd.parser.remove_option('--dry-run')

    def test_dry_run_no_booelan(self, mock_super_load_config, mock_get_action, mock_model,
                                mock_triplestore_is_available):
        '''Calls the triplestore command with invalid dry-run flag.'''

        gather_mock = Mock('gather_dataset_ids')
        gather_mock.return_value = []
        self.cmd._gather_dataset_ids = gather_mock
        self.cmd.args = ['reindex']
        self.cmd.options.dry_run = 'F'

        with self.assertRaises(SystemExit) as cm:
            self.cmd.command()

        # verify
        self.assertEqual(cm.exception.code, 2)

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        gather_mock.assert_not_called()

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 1)

    def test_dry_run_default_no_datasets(self, mock_super_load_config, mock_get_action, mock_model,
                                         mock_triplestore_is_available):
        '''Calls the triplestore command with the dry-run flag True. No dataset.'''

        mock_triplestore_is_available.return_value = False
        gather_mock = Mock('gather_dataset_ids')
        gather_mock.return_value = []
        self.cmd._gather_dataset_ids = gather_mock
        self.cmd.args = ['reindex']

        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        gather_mock.assert_called_once_with()
        self.assertEqual(mock_triplestore_is_available.call_count, 1)

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 1)

    def test_dry_run_default_two_datasets(self, mock_super_load_config, mock_get_action, mock_model,
                                          mock_triplestore_is_available):
        '''Calls the triplestore command without the dry-run flag True. Two datasets.'''

        mock_triplestore_is_available.return_value = False
        gather_mock = Mock('gather_dataset_ids')
        gather_mock.return_value = ['d1', 'd2']
        self.cmd._gather_dataset_ids = gather_mock
        self.cmd.args = ['reindex']

        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        gather_mock.assert_called_once_with()
        self.assertEqual(mock_triplestore_is_available.call_count, 1)

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 1)

    def test_dry_run_false_fuseki_client_available(self, mock_super_load_config, mock_get_action, mock_model,
                                mock_triplestore_is_available):
        '''Calls the triplestore command with the dry-run flag and false. Fuseki client available.'''

        mock_triplestore_is_available.side_effect = [False, True]
        gather_mock = Mock('gather_dataset_ids')
        gather_mock.return_value = ['d1', 'd2']
        self.cmd._gather_dataset_ids = gather_mock
        # TODO : Set return values for mock_get_action 'dcat_dataset_show' calls
        self.cmd.options.dry_run = 'False'
        self.cmd.args = ['reindex']

        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        gather_mock.assert_called_once_with()
        self.assertEqual(mock_triplestore_is_available.call_count, 2)

        # assert that the needed methods were obtained in the expected order.
        # TODO : Detailed assert of all calls
        # mock_get_action.assert_has_calls([call('get_site_user'), call('dcat_dataset_show'),
        #                                  call('dcat_dataset_show')])
        self.assertEqual(mock_get_action.call_count, 3)

    def test_dry_run_false_fuseki_client_not_available(self, mock_super_load_config, mock_get_action,
                                                       mock_model, mock_triplestore_is_available):
        '''Calls the triplestore command with the dry-run flag and false. Fuseki client unavailable.'''

        mock_triplestore_is_available.return_value = False
        gather_mock = Mock('gather_dataset_ids')
        gather_mock.return_value = ['d1', 'd2']
        self.cmd._gather_dataset_ids = gather_mock
        self.cmd.options.dry_run = 'False'
        self.cmd.args = ['reindex']

        self.cmd.command()

        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        gather_mock.assert_called_once_with()
        self.assertEqual(mock_triplestore_is_available.call_count, 2)

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 1)
