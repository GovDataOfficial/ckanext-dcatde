#!/usr/bin/python
# -*- coding: utf8 -*-

from collections import OrderedDict
import unittest

import ckanext.dcatde.commands.command_util as utils
import ckanext.dcatde.tests.commands.common_helpers as helpers
from ckanext.dcatde.triplestore.fuseki_client import FusekiTriplestoreClient
from ckanext.dcatde.validation.shacl_validation import ShaclValidator
from mock import patch, call, Mock, MagicMock
from rdflib import URIRef


class DummyClass:
    pass


@patch("ckanext.dcatde.dataset_utils.gather_dataset_ids")
@patch("ckanext.dcatde.validation.shacl_validation.ShaclValidator.validate")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore_mqa")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore_mqa")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.create_dataset_in_triplestore")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.delete_dataset_in_triplestore")
@patch("ckanext.dcatde.triplestore.fuseki_client.FusekiTriplestoreClient.is_available")
@patch("ckan.plugins.toolkit.get_action")
class TestTripleStoreCommand(unittest.TestCase):
    '''Tests the CKAN DCATde triplestore command.'''


    @staticmethod
    def _get_serialized_rdf(uri, contributor_id=None):
        rdf = '''@prefix dcat: <http://www.w3.org/ns/dcat#> .
        
        <{uri}> a dcat:Dataset .'''.format(uri=uri)
        if contributor_id:
            rdf = '''@prefix dcatde: <http://dcat-ap.de/def/dcatde/> .
            ''' + rdf + '''
            <{uri}> dcatde:contributorID <{contributor_id}> .
            '''.format(uri=uri, contributor_id=contributor_id)
        return rdf


    def test_dry_run_default_no_datasets(self, mock_get_action,
                                         mock_triplestore_is_available, mock_triplestore_delete,
                                         mock_triplestore_create, mock_triplestore_delete_mqa,
                                         mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        '''Calls the triplestore command with the dry-run flag True. No dataset.'''

        #prepare
        mock_gather_ids.return_value = {}
        triplestore_client = FusekiTriplestoreClient()
        shacl_validation_client = ShaclValidator()
        admin_user = dict(name='admin')
        dry_run = True

        #execute
        utils.reindex(dry_run, triplestore_client, shacl_validation_client, admin_user)

        #verify
        # not called, DRY-RUN
        mock_triplestore_is_available.assert_not_called()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_triplestore_delete_mqa.assert_not_called()
        mock_triplestore_create_mqa.assert_not_called()
        mock_gather_ids.assert_called_once_with(include_private=False)

    def test_dry_run_default_two_datasets(self, mock_get_action,
                                          mock_triplestore_is_available, mock_triplestore_delete,
                                          mock_triplestore_create, mock_triplestore_delete_mqa,
                                          mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        '''Calls the triplestore command without the dry-run flag True. Two datasets.'''

        #prepare
        mock_gather_ids.return_value = dict(d1='org-1', d2='org-2')
        triplestore_client = FusekiTriplestoreClient()
        shacl_validation_client = ShaclValidator()
        admin_user = dict(name='admin')
        dry_run = True

        #execute
        utils.reindex(dry_run, triplestore_client, shacl_validation_client, admin_user)

        #verify
        # not called, DRY-RUN
        mock_triplestore_is_available.assert_not_called()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_triplestore_delete_mqa.assert_not_called()
        mock_triplestore_create_mqa.assert_not_called()
        mock_gather_ids.assert_called_once_with(include_private=False)

    def test_dry_run_false_fuseki_client_available(self, mock_get_action,
                                mock_triplestore_is_available, mock_triplestore_delete,
                                mock_triplestore_create, mock_triplestore_delete_mqa,
                                mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        '''Calls the triplestore command with the dry-run flag and false. Fuseki client available.'''

        #prepare
        mock_triplestore_is_available.return_value = True
        uri_d1 = 'http://ckan.govdata.de/dataset/d1'
        d1 = dict(rdf=self._get_serialized_rdf(uri_d1), org='org-d1', shacl_result='ValidationReport-d1')
        uri_d2 = 'http://ckan.govdata.de/dataset/d2'
        contributor_id_d2 = 'http://dcat-ap.de/def/contributors/test'
        d2 = dict(rdf=self._get_serialized_rdf(uri_d2, contributor_id_d2), org='org-d2',
                  shacl_result='ValidationReport-d2')
        action_hlp = helpers.GetActionHelper()
        action_hlp.return_val_actions['get_site_user'] = dict(name='admin')
        action_hlp.side_effect_actions['dcat_dataset_show'] = [d1['rdf'], d2['rdf']]
        action_hlp.build_mocks()
        mock_get_action.side_effect = action_hlp.mock_get_action
        mock_shacl_validate.side_effect = [d1['shacl_result'], d2['shacl_result']]
        package_ids_with_org = dict(d1=d1['org'], d2=d2['org'])
        # use sorted dict because of expected order
        mock_gather_ids.return_value = OrderedDict(sorted(iter(package_ids_with_org.items()),
                                                          key=lambda x: x[1]))

        triplestore_client = FusekiTriplestoreClient()
        shacl_validation_client = ShaclValidator()
        admin_user = dict(name='admin')
        dry_run = False

        #execute
        utils.reindex(dry_run, triplestore_client, shacl_validation_client, admin_user)

        #verify
        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_has_calls([call(URIRef(uri_d1)), call(URIRef(uri_d2))])
        mock_triplestore_create.assert_has_calls([call(d1['rdf'], URIRef(uri_d1)),
                                                  call(d2['rdf'], URIRef(uri_d2))])
        mock_shacl_validate.assert_has_calls([call(d1['rdf'], URIRef(uri_d1), d1['org'], None),
                                              call(d2['rdf'], URIRef(uri_d2), d2['org'], contributor_id_d2)])
        mock_triplestore_delete_mqa.assert_has_calls([call(URIRef(uri_d1)), call(URIRef(uri_d2))])
        mock_triplestore_create_mqa.assert_has_calls([call(d1['shacl_result'], URIRef(uri_d1)),
                                                      call(d2['shacl_result'], URIRef(uri_d2))])
        mock_gather_ids.assert_called_once_with(include_private=False)
        mock_get_action.assert_has_calls([ call('dcat_dataset_show'),
                                          call('dcat_dataset_show')])

    def test_dry_run_false_fuseki_client_not_available(self, mock_get_action,
                                                       mock_triplestore_is_available,
                                                       mock_triplestore_delete, mock_triplestore_create,
                                                       mock_triplestore_delete_mqa,
                                                       mock_triplestore_create_mqa, mock_shacl_validate,
                                                       mock_gather_ids):
        '''Calls the triplestore command with the dry-run flag and false. Fuseki client unavailable.'''

        #prepare
        mock_triplestore_is_available.return_value = False
        mock_gather_ids.return_value = dict(d1='org-1', d2='org-2')
        triplestore_client = FusekiTriplestoreClient()
        shacl_validation_client = ShaclValidator()
        admin_user = dict(name='admin')
        dry_run = False

        #execute
        utils.reindex(dry_run, triplestore_client, shacl_validation_client, admin_user)

        #verify
        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()
        mock_shacl_validate.assert_not_called()
        mock_triplestore_delete_mqa.assert_not_called()
        mock_triplestore_create_mqa.assert_not_called()
        mock_gather_ids.assert_called_once_with(include_private=False)


    def test_delete_datasets_dry_run(self, mock_get_action,
                                    mock_triplestore_is_available, mock_triplestore_delete,
                                    mock_triplestore_create, mock_triplestore_delete_mqa,
                                    mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        ''' Call delete datasets with dry run'''

        #prepare
        mock_triplestore_is_available.return_value = True
        uris_to_clean = ['123','234']
        dry_run = True
        triplestore_client = FusekiTriplestoreClient()

        #execute
        utils.clean_triplestore_from_uris(dry_run, triplestore_client, uris_to_clean)

        #verify
        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()


    def test_delete_datasets_no_uris(self, mock_get_action,
                                    mock_triplestore_is_available, mock_triplestore_delete,
                                    mock_triplestore_create, mock_triplestore_delete_mqa,
                                    mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        ''' Call delete datasets without URIs'''

        #prepare
        mock_triplestore_is_available.return_value = True
        uris_to_clean = ''
        dry_run = False
        triplestore_client = FusekiTriplestoreClient()

        #execute
        utils.clean_triplestore_from_uris(dry_run, triplestore_client, uris_to_clean)

        #verify
        mock_triplestore_is_available.assert_not_called()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()

    def test_delete_datasets_triplestore_not_available(self, mock_get_action,
                                    mock_triplestore_is_available, mock_triplestore_delete,
                                    mock_triplestore_create, mock_triplestore_delete_mqa,
                                    mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        ''' Call delete datasets when triplestore is not available'''

        #prepare
        mock_triplestore_is_available.return_value = False
        triplestore_client = FusekiTriplestoreClient()
        uris_to_clean = ['123','234']
        dry_run = False

        #execute
        utils.clean_triplestore_from_uris(dry_run, triplestore_client, uris_to_clean)

        #verify
        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_not_called()
        mock_triplestore_create.assert_not_called()

    def test_delete_datasets_success(self, mock_get_action,
                                    mock_triplestore_is_available, mock_triplestore_delete,
                                    mock_triplestore_create, mock_triplestore_delete_mqa,
                                    mock_triplestore_create_mqa, mock_shacl_validate, mock_gather_ids):
        ''' Call delete datasets'''

        #prepare
        mock_triplestore_is_available.return_value = True
        triplestore_client = FusekiTriplestoreClient()
        uris_to_clean = ['123','234']
        dry_run = False

        #execute
        utils.clean_triplestore_from_uris(dry_run, triplestore_client, uris_to_clean)

        #verify
        mock_triplestore_is_available.assert_called_once_with()
        mock_triplestore_delete.assert_has_calls([call('123'), call('234')])
        mock_triplestore_create.assert_not_called()
