#!/usr/bin/python
# -*- coding: utf8 -*-

from collections import OrderedDict
import unittest

from ckanext.dcatde.commands.triplestore import Triplestore
import ckanext.dcatde.tests.commands.common_helpers as helpers
from mock import patch, call, Mock, MagicMock
from rdflib import URIRef


class DummyClass:
    pass


@patch("ckanext.dcatde.commands.click.triplestore.gather_dataset_ids")
@patch("ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available")
@patch("ckanext.dcatde.commands.triplestore.model", autospec=True)
@patch("ckan.plugins.toolkit.get_action")
@patch("ckanext.dcatde.commands.cli.CkanCommand._load_config")
class TestTripleStoreCommand(unittest.TestCase):
    '''Tests the CKAN DCATde triplestore command.'''

    def setUp(self):
        self.cmd = Triplestore(name='TripleStoreTest')
        self.cmd.args = []
        self.cmd.options = DummyClass()
        self.cmd.options.dry_run = 'True'
        self.cmd.options.uris = ''

    def tearDown(self):
        # Remove option to avoid OptionConflictError
        self.cmd.parser.remove_option('--dry-run')
        self.cmd.parser.remove_option('--uris')

    @staticmethod
    def _get_rdf(uri):
        rdf = '''@prefix dcat: <http://www.w3.org/ns/dcat#> .
        
        <%s> a dcat:Dataset .
        ''' % uri
        return rdf

    def test_dry_run_no_booelan(self, mock_super_load_config, mock_get_action, mock_model,
                                mock_triplestore_is_available, mock_triplestore_delete,
                                mock_triplestore_create, mock_triplestore_delete_mqa,
                                mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        '''Calls the triplestore command with invalid dry-run flag.'''

        mock_gather_ids.return_value = {}
        self.cmd.args = ['reindex']
        self.cmd.options.dry_run = 'None'

        with self.assertRaises(SystemExit) as cm:
            self.cmd.command()

        # verify
        self.assertEqual(cm.exception.code, 2)

        # not called, DRY-RUN
        mock_triplestore_is_available.assert_not_called()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_triplestore_delete_mqa.assert_not_called()
        mock_triplestore_create_mqa.assert_not_called()
        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 1)

    def test_dry_run_default_no_datasets(self, mock_super_load_config, mock_get_action, mock_model,
                                         mock_triplestore_is_available, mock_triplestore_delete,
                                         mock_triplestore_create, mock_triplestore_delete_mqa,
                                         mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        '''Calls the triplestore command with the dry-run flag True. No dataset.'''

        mock_gather_ids.return_value = {}
        self.cmd.args = ['reindex']

        self.cmd.command()

        # not called, DRY-RUN
        mock_triplestore_is_available.assert_not_called()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_triplestore_delete_mqa.assert_not_called()
        mock_triplestore_create_mqa.assert_not_called()
        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with(include_private=False)

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 1)

    def test_dry_run_default_two_datasets(self, mock_super_load_config, mock_get_action, mock_model,
                                          mock_triplestore_is_available, mock_triplestore_delete,
                                          mock_triplestore_create, mock_triplestore_delete_mqa,
                                          mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        '''Calls the triplestore command without the dry-run flag True. Two datasets.'''

        mock_gather_ids.return_value = dict(d1='org-1', d2='org-2')
        self.cmd.args = ['reindex']
        # #
        self.cmd.command()

        # not called, DRY-RUN
        mock_triplestore_is_available.assert_not_called()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_triplestore_delete_mqa.assert_not_called()
        mock_triplestore_create_mqa.assert_not_called()
        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with(include_private=False)

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 1)

    def test_dry_run_false_fuseki_client_available(self, mock_super_load_config, mock_get_action, mock_model,
                                mock_triplestore_is_available, mock_triplestore_delete,
                                mock_triplestore_create, mock_triplestore_delete_mqa,
                                mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        '''Calls the triplestore command with the dry-run flag and false. Fuseki client available.'''

        mock_triplestore_is_available.return_value = True
        uri_d1 = 'http://ckan.govdata.de/dataset/d1'
        d1 = dict(rdf=self._get_rdf(uri_d1), org='org-d1', shacl_result='ValidationReport-d1')
        uri_d2 = 'http://ckan.govdata.de/dataset/d2'
        d2 = dict(rdf=self._get_rdf(uri_d2), org='org-d2', shacl_result='ValidationReport-d2')
        action_hlp = helpers.GetActionHelper()
        action_hlp.return_val_actions['get_site_user'] = dict(name='admin')
        action_hlp.side_effect_actions['dcat_dataset_show'] = [d1['rdf'], d2['rdf']]
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action
        mock_shacl_validate.side_effect = [d1['shacl_result'], d2['shacl_result']]
        package_ids_with_org = dict(d1=d1['org'], d2=d2['org'])
        # use sorted dict because of expected order
        mock_gather_ids.return_value = OrderedDict(sorted(package_ids_with_org.items(),
                                                          key=lambda x: x[1]))
        self.cmd.options.dry_run = 'False'
        self.cmd.args = ['reindex']

        self.cmd.command()

        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_has_calls([call(URIRef(uri_d1)), call(URIRef(uri_d2))])
        mock_triplestore_create.assert_has_calls([call(d1['rdf'], URIRef(uri_d1)),
                                                  call(d2['rdf'], URIRef(uri_d2))])
        mock_shacl_validate.assert_has_calls([call(d1['rdf'], URIRef(uri_d1), d1['org']),
                                              call(d2['rdf'], URIRef(uri_d2), d2['org'])])
        mock_triplestore_delete_mqa.assert_has_calls([call(URIRef(uri_d1), d1['org']),
                                                      call(URIRef(uri_d2), d2['org'])])
        mock_triplestore_create_mqa.assert_has_calls([call(d1['shacl_result'], URIRef(uri_d1)),
                                                      call(d2['shacl_result'], URIRef(uri_d2))])
        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with(include_private=False)

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user'), call('dcat_dataset_show'),
                                          call('dcat_dataset_show')])

    def test_dry_run_false_fuseki_client_not_available(self, mock_super_load_config, mock_get_action,
                                                       mock_model, mock_triplestore_is_available,
                                                       mock_triplestore_delete, mock_triplestore_create,
                                                       mock_triplestore_delete_mqa,
                                                       mock_triplestore_create_mqa, mock_shacl_validate,
                                                       mock_gather_ids):
        '''Calls the triplestore command with the dry-run flag and false. Fuseki client unavailable.'''

        mock_triplestore_is_available.return_value = False
        mock_gather_ids.return_value = dict(d1='org-1', d2='org-2')
        self.cmd.options.dry_run = 'False'
        self.cmd.args = ['reindex']

        self.cmd.command()

        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_triplestore_delete_mqa.assert_not_called()
        mock_triplestore_create_mqa.assert_not_called()
        # ensure config was loaded
        mock_super_load_config.assert_called_once_with()
        mock_gather_ids.assert_called_once_with(include_private=False)

        # assert that the needed methods were obtained in the expected order.
        mock_get_action.assert_has_calls([call('get_site_user')])
        self.assertEqual(mock_get_action.call_count, 1)

    def test_delete_datasets_dry_run(self, mock_super_load_config, mock_get_action, mock_model,
                                    mock_triplestore_is_available, mock_triplestore_delete,
                                    mock_triplestore_create, mock_triplestore_delete_mqa,
                                    mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        ''' Call delete datasets with dry run'''
        mock_triplestore_is_available.return_value = True

        self.cmd.options.dry_run = 'True'
        self.cmd.options.uris = '123,234'
        self.cmd.args = ['delete_datasets']

        self.cmd.command()

        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()


    def test_delete_datasets_no_uris(self, mock_super_load_config, mock_get_action, mock_model,
                                    mock_triplestore_is_available, mock_triplestore_delete,
                                    mock_triplestore_create, mock_triplestore_delete_mqa,
                                    mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        ''' Call delete datasets without URIs'''
        mock_triplestore_is_available.return_value = True

        self.cmd.options.dry_run = 'False'
        self.cmd.args = ['delete_datasets']

        self.cmd.command()

        mock_triplestore_is_available.assert_not_called()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()

    def test_delete_datasets_triplestore_not_available(self, mock_super_load_config, mock_get_action, mock_model,
                                    mock_triplestore_is_available, mock_triplestore_delete,
                                    mock_triplestore_create, mock_triplestore_delete_mqa,
                                    mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        ''' Call delete datasets when triplestore is not available'''
        mock_triplestore_is_available.return_value = False

        self.cmd.options.dry_run = 'False'
        self.cmd.options.uris = '123,234'
        self.cmd.args = ['delete_datasets']

        self.cmd.command()

        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()

    def test_delete_datasets_success(self, mock_super_load_config, mock_get_action, mock_model,
                                    mock_triplestore_is_available, mock_triplestore_delete,
                                    mock_triplestore_create, mock_triplestore_delete_mqa,
                                    mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        ''' Call delete datasets'''
        mock_triplestore_is_available.return_value = True

        self.cmd.options.dry_run = 'False'
        self.cmd.options.uris = '123,234'
        self.cmd.args = ['delete_datasets']

        self.cmd.command()

        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_has_calls([call('123'), call('234')])
        mock_triplestore_create.assert_not_called()
