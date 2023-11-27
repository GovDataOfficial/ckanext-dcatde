#!/usr/bin/python
# -*- coding: utf8 -*-

import unittest
from ckanext.dcatde.dataset_utils import gather_dataset_ids
from mock import patch, call, Mock, MagicMock, ANY
from ckantoolkit.tests import helpers

class DummyNot():
    def filter():
        return None

class DummyQuery():

    def __init__(self, val, filter_val):
        self.value = val
        self.filter_value = filter_val
        self.count = 0

    def __str__(self):
        return self.value

    def filter(self, a):
        self.count = self.count + 1
        return self.filter_value

    def getCallCount(self):
        return self.count

@patch("sqlalchemy.orm.query")
@patch("ckanext.dcatde.dataset_utils.or_")
@patch("ckanext.dcatde.dataset_utils.and_")
@patch("ckanext.dcatde.dataset_utils.not_")
@patch("ckanext.dcatde.dataset_utils.aliased")
@patch("ckanext.harvest.model.HarvestSource")
@patch("ckanext.dcatde.dataset_utils.model")
class DatasetUtils(unittest.TestCase):
    """ Test Util functions """

    @helpers.change_config('ckan.plugins', 'harvest activity')
    def test_gather_dataset_ids_with_private_datasets(self, mock_model, mock_harvest_source, mock_aliased,
                                                        mock_not, mock_and, mock_or, mock_query_filter):
        """ Test gather_dataset_ids() with parameter include_private==True """
        
        packages_in_db = [('id-1', "URI-1"), ('id-2', "URI-2"), ('id-3', "URI-3")]
        expected_result = {}
        for row in packages_in_db:
            expected_result[row[0]] = row[1]

        mock_not.return_value = DummyNot()
        mock_query_result = Mock(name='query-result')
        mock_query_result.distinct().outerjoin().filter().filter().filter.return_value = packages_in_db

        mock_query = Mock(name='query')
        mock_query.side_effect = [mock_query_result, mock_query, mock_not]
        mock_model.Session.query = mock_query

        res = gather_dataset_ids(include_private=True)

        self.assertEqual(res, expected_result)
        self.assertEqual(mock_query.call_count, 3)

    @helpers.change_config('ckan.plugins', 'harvest activity')
    def test_gather_dataset_ids_without_private_datasets(self, mock_model, mock_harvest_source, mock_aliased,
                                                            mock_not, mock_and, mock_or, mock_query_filter):
        """ Test gather_dataset_ids() with parameter include_private==False """
        
        packages_filtered = [('id-1', "URI-1"), ('id-2', "URI-2")]
        expected_result = {}
        for row in packages_filtered:
            expected_result[row[0]] = row[1]

        return_query = DummyQuery([], packages_filtered)
        mock_not.return_value = DummyNot()
        mock_query_result = Mock(name='query-result')
        mock_query_result.distinct().outerjoin().filter().filter().filter.return_value = return_query
        mock_query = Mock(name='query')
        mock_query.side_effect = [mock_query_result, mock_query, mock_not]
        mock_model.Session.query = mock_query

        res = gather_dataset_ids(include_private=False)

        self.assertEqual(res, expected_result)
        self.assertEqual(return_query.getCallCount(), 1)
        self.assertEqual(mock_query.call_count, 3)

    @helpers.change_config('ckan.plugins', 'activity')
    def test_gather_dataset_ids_with_private_datasets_no_harvest_plugin(self, mock_model, mock_harvest_source, mock_aliased,
                                                            mock_not, mock_and, mock_or, mock_query_filter):
        """
        Test gather_dataset_ids() with parameter include_private==True
        when harvest plugin is not enabled
        """

        packages_in_db = [('id-1', "URI-1"), ('id-2', "URI-2"), ('id-3', "URI-3")]
        expected_result = {}
        for row in packages_in_db:
            expected_result[row[0]] = row[1]

        mock_query_result = Mock(name='query-result')
        mock_query_result.distinct().outerjoin().filter().filter.return_value = packages_in_db

        mock_query = Mock(name='query')
        mock_query.side_effect = [mock_query_result]
        mock_model.Session.query = mock_query

        res = gather_dataset_ids(include_private=True)

        self.assertEqual(res, expected_result)
        self.assertEqual(mock_query.call_count, 1)

    @helpers.change_config('ckan.plugins', 'activity')
    def test_gather_dataset_ids_without_private_datasets_no_harvest_plugin(self, mock_model, mock_harvest_source, mock_aliased,
                                                            mock_not, mock_and, mock_or, mock_query_filter):
        """
        Test gather_dataset_ids() with parameter include_private==False
        when harvest plugin is not enabled
        """

        packages_filtered = [('id-1', "URI-1"), ('id-2', "URI-2")]
        expected_result = {}
        for row in packages_filtered:
            expected_result[row[0]] = row[1]

        return_query = DummyQuery([], packages_filtered)
        mock_query_result = Mock(name='query-result')
        mock_query_result.distinct().outerjoin().filter().filter.return_value = return_query
        mock_query = Mock(name='query')
        mock_query.side_effect = [mock_query_result]
        mock_model.Session.query = mock_query

        res = gather_dataset_ids(include_private=False)

        self.assertEqual(res, expected_result)
        self.assertEqual(return_query.getCallCount(), 1)
        self.assertEqual(mock_query.call_count, 1)