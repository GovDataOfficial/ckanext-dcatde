# -*- coding: utf8 -*-

import unittest
import six
import ckanext.dcatde.migration.migration_functions as mf


class TestMigrationFunctions(unittest.TestCase):
    '''
    Unit tests for dcatde.migraition.migration_functions
    '''

    def setUp(self):
        license_mapping = {
            'ogd-license-key': 'dcatde-license-key'
        }

        category_mapping = {
            "bevoelkerung": "soci",
            "bildung_wissenschaft": ["educ", "tech"],
            "geo": None
        }

        self.migrations = mf.MigrationFunctions(license_mapping, category_mapping)

    def _build_dataset_extras(self, key, value):
        '''
        creates a test dict containing an extras field with key and value,
        and another extras field which should not be changed
        '''
        return {
            u'extras': [
                {u'key': key,
                 u'value': value},
                {u'key': u'unaffected-test-key',
                 u'value': u'unaffected-test-value'}
            ]
        }

    def _build_dataset_extras_contacts(self, role, additional_keys=u'',
                                       additional_dict=True):
        '''
        creates a test dict containing a contacts field having the given
        role
        '''
        if additional_keys:
            additional_keys += u','

        if additional_dict:
            additional_entry = u', {"keep": "me"}'
        else:
            additional_entry = u''

        return self._build_dataset_extras(
            u'contacts',
            u'[{"role": "' + role + u'",' + additional_keys +
            u' "name": "testname",' +
            u' "email": "mail@me.at",' +
            u' "url": "http://example.com/"' +
            u'}' + additional_entry + u']')

    def _build_dataset_extras_dates(self, role, additional_dict=True):
        '''
        creates a test dict containing a dates field having the given
        role
        '''
        if additional_dict:
            additional_entry = u', {"keep": "me"}'
        else:
            additional_entry = u''

        return self._build_dataset_extras(
            u'dates',
            u'[{"role": "' + role + '",' +
            u' "date": "2016-04-01T00:00:00"' +
            u'}' + additional_entry + ']')

    def _assert_dataset_len(self, dataset, length):
        '''
        asserts that the given dataset has size length
        '''
        self.assertTrue(len(dataset) == length,
                        'expected ' + str(length) + ' elements in dataset, ' +
                        'but has ' + str(len(dataset)))

    def _assert_extras_len(self, dataset, length):
        '''
        asserts that the given dataset's extas field has the given
        number of elements
        '''
        self.assertTrue(len(dataset[u'extras']) == length,
                        'expected ' + str(length) + ' extras field(s), ' +
                        'but has ' + str(len(dataset[u'extras'])))

    def _assert_extras_key(self, dataset, key):
        '''
        asserts that the dataset's extras field contains the given
        key and returns its value
        '''
        self.assertIn(u'extras', dataset)
        for item in dataset[u'extras']:
            if item[u'key'] == key:
                return item[u'value']

        self.fail('Key "' + key + '" not present in extras field')

    def _assert_extras_key_value(self, dataset, key, value):
        '''
        asserts that the dataset's extras field contains the given
        key - value pair
        '''
        real_val = self._assert_extras_key(dataset, key)
        self.assertTrue(real_val == value,
                        u'expected value "' + six.text_type(value)
                        + u'", but was "' + six.text_type(real_val)
                        + u'" for key ' + six.text_type(key))

    def _assert_extras_other_unaffected(self, dataset):
        '''
        checks if the additional key added by _build_dataset_extras
        is still the same
        '''
        self._assert_extras_key_value(dataset,
                                      u'unaffected-test-key',
                                      u'unaffected-test-value')

    def _assert_contacts_keep_testdata(self, dataset):
        '''
        checks if the additional extras data and the additional
        contacts data were unaffected
        '''
        self._assert_extras_other_unaffected(dataset)
        self._assert_extras_key_value(dataset, u'contacts',
                                      u'[{"keep": "me"}]')

    def _assert_dates_keep_testdata(self, dataset):
        '''
        checks if the additional extras data and the additional
        dates data were unaffected
        '''
        self._assert_extras_other_unaffected(dataset)
        self._assert_extras_key_value(dataset, u'dates',
                                      u'[{"keep": "me"}]')

    def _assert_date_in(self, dataset, extras_field):
        '''
        checks if the test date from _build_dataset_extras_dates
        is present in the given extras field
        '''
        self._assert_extras_key_value(dataset, extras_field,
                                      u'2016-04-01T00:00:00')

    def _assert_correct_extras(self, dataset, num_items=1):
        '''
        Convenience function for asserts on datasets created by
        _build_dataset_extras. It checks if the additional item was
        unaffected and if num_items further extras fields are present.
        '''
        self._assert_dataset_len(dataset, 1)
        self._assert_extras_len(dataset, num_items + 1)
        self._assert_extras_other_unaffected(dataset)

    def test_metadata_original_portal(self):
        test_ds = self._build_dataset_extras(u'metadata_original_portal',
                                             u'test')

        self.migrations.metadata_original_portal(test_ds)

        # new field plus metadata_harvested_portal
        self._assert_correct_extras(test_ds, 2)
        self._assert_extras_key_value(test_ds, u'contributorID', u'["test"]')
        self._assert_extras_key_value(test_ds, u'metadata_harvested_portal', u'test')

    def test_metadata_original_id(self):
        test_ds = self._build_dataset_extras(u'metadata_original_id',
                                             u'my id')

        self.migrations.metadata_original_id(test_ds)

        self._assert_correct_extras(test_ds)
        self._assert_extras_key_value(test_ds, u'identifier',
                                      u'my id')

    def test_spatial_reference_text(self):
        test_ds = self._build_dataset_extras(u'spatial_reference',
                                             u'{"text": "ref"}')

        self.migrations.spatial_reference_text(test_ds)

        self._assert_correct_extras(test_ds, 2)
        self._assert_extras_key_value(test_ds, u'geocodingText', u'["ref"]')
        self._assert_extras_key_value(test_ds, u'spatial_reference', u'{}')

    def test_groups(self):
        test_ds = {'groups': [{'name': 'bevoelkerung', 'id': 'bevoelkerung'},
                              {'name': 'bildung_wissenschaft', 'id': 'bildung_wissenschaft'},
                              {'name': 'geo', 'id': 'geo'}]}

        self.migrations.groups(test_ds)

        self.assertListEqual(test_ds['groups'],
                             [{'name': 'soci', 'id': 'soci'},
                              {'name': 'educ', 'id': 'educ'},
                              {'name': 'tech', 'id': 'tech'}],
                             "Group mapping failed")

    def test_temporal_coverage_from(self):
        test_ds = self._build_dataset_extras(u'temporal_coverage_from',
                                             u'1234')

        self.migrations.temporal_coverage_from(test_ds)

        self._assert_correct_extras(test_ds)
        self._assert_extras_key_value(test_ds, u'temporal_start', u'1234')

    def test_temporal_coverage_to(self):
        test_ds = self._build_dataset_extras(u'temporal_coverage_to',
                                             u'5678')

        self.migrations.temporal_coverage_to(test_ds)

        self._assert_correct_extras(test_ds)
        self._assert_extras_key_value(test_ds, u'temporal_end', u'5678')

    def test_geographical_granularity(self):
        dc_base = u'http://dcat-ap.de/def/politicalGeocoding/Level/'

        # test the 4 known mappings
        self._run_geographical_granularity(u'bund', dc_base + u'federal')
        self._run_geographical_granularity(u'land', dc_base + u'state')
        self._run_geographical_granularity(u'kommune',
                                           dc_base + u'municipality')
        self._run_geographical_granularity(u'stadt',
                                           dc_base + u'municipality')

        # test DCAT value mappings (without URI part)
        self._run_geographical_granularity(u'federal', dc_base + u'federal')
        self._run_geographical_granularity(u'state', dc_base + u'state')
        self._run_geographical_granularity(u'municipality', dc_base + u'municipality')

        # test additional non-OGD value
        self._run_geographical_granularity(u'kreis', dc_base + u'administrativeDistrict')

        # test unknown mapping
        self._run_geographical_granularity(u'something-different',
                                           u'something-different')

    def _run_geographical_granularity(self, from_val, to_val):
        test_ds = self._build_dataset_extras(u'geographical_granularity',
                                             from_val)

        self.migrations.geographical_granularity(test_ds)

        self._assert_correct_extras(test_ds)
        self._assert_extras_key_value(test_ds, u'politicalGeocodingLevelURI',
                                      to_val)

    def _run_address_migration_with(self, role_from, role_to, target_func):
        test_addrs = {
            u'Adenauerallee 99-103, 53113 Bonn': {
                'street': u'Adenauerallee 99-103',
                'zip': u'53113',
                'city': u'Bonn'
            },
            u'Hansestadt Rostock;Kataster-, Vermessungs- und Liegenschaftsamt;Holbeinplatz 14;18069 Rostock;DE': {
                'addressee': u'Hansestadt Rostock',
                'details': u'Kataster-, Vermessungs- und Liegenschaftsamt',
                'street': u'Holbeinplatz 14',
                'zip': u'18069',
                'city': u'Rostock',
                'country': u'DE'
            },
            u'Schwannstr. 3, D-40476, Düsseldorf, DEU': {
                'street': u'Schwannstr. 3',
                'zip': u'D-40476',
                'city': u'Düsseldorf',
                'country': u'DEU'
            },
            u'OpenStreetMap Foundation;132 Maney Hill Road;Sutton Coldfield;West Midlands;B72 1JU;United Kingdom': {
                'addressee': u'OpenStreetMap Foundation',
                'street': u'132 Maney Hill Road',
                'zip': u'B72 1JU',
                'country': u'United Kingdom',
                'not_mapped': u'Sutton Coldfield, West Midlands'
            }
        }

        for addr in test_addrs:
            test_ds = self._build_dataset_extras_contacts(
                        role_from, u'"address" :"' + addr + u'"')

            target_func(test_ds)

            # remove the not_mapped key from the mapping first
            expected = test_addrs[addr]
            remains = expected.pop('not_mapped', None)

            for key in expected:
                # check that all fields are present as expected
                # (i.e. with a correct role prefix)
                self._assert_extras_key_value(test_ds, role_to + '_' + key,
                                              expected[key])

            if remains:
                # old field is still present and contains remaining data
                data = self._assert_extras_key(test_ds, u'contacts')
                self.assertTrue(role_from in data, u'old key not present')
                self.assertTrue(u'"address"' in data, u'No address field')
                self.assertTrue(remains in data, u'remaining address not found')
            else:
                # assert that the old entry has been deleted
                self._assert_contacts_keep_testdata(test_ds)

    def test_contacts_role_autor(self):
        test_ds = self._build_dataset_extras_contacts(u'autor')

        # expect: unaffected, contacts, author_url, author_contacttype
        self._run_contacts_role_autor_with(test_ds, 4)

        self._assert_contacts_keep_testdata(test_ds)

    def test_contacts_role_autor_data(self):
        test_ds = self._build_dataset_extras_contacts(u'autor')
        test_ds[u'author'] = u'oldvalue'
        test_ds[u'author_email'] = u'oldvalue@example.com'

        # expect: unaffected, contacts, author_url, author_contacttype
        self._run_contacts_role_autor_with(test_ds, 4)

        self._assert_contacts_keep_testdata(test_ds)

    def test_contacts_role_autor_lastfield(self):
        test_ds = self._build_dataset_extras_contacts(u'autor', additional_dict=False)

        # expect: unaffected, author_url, author_contacttype
        self._run_contacts_role_autor_with(test_ds, 3)

        self._assert_extras_other_unaffected(test_ds)

    def test_contacts_role_autor_address(self):
        self._run_address_migration_with(u'autor', u'author',
                                         self.migrations.contacts_role_autor)

    def _run_contacts_role_autor_with(self, test_ds, extras_len):
        self.migrations.contacts_role_autor(test_ds)

        # expect: extras, author, author_email
        self._assert_dataset_len(test_ds, 3)
        self.assertDictContainsSubset({u'author': u'testname'}, test_ds)
        self.assertDictContainsSubset({u'author_email': u'mail@me.at'},
                                      test_ds)
        self._assert_extras_len(test_ds, extras_len)
        self._assert_extras_key_value(test_ds, u'author_url',
                                      u'http://example.com/')
        self._assert_extras_key_value(test_ds, u'author_contacttype',
                                      u'Organization')

    def test_contacts_role_ansprechpartner(self):
        test_ds = self._build_dataset_extras_contacts(u'ansprechpartner')

        self._run_contacts_role_ansprechpartner_with(test_ds)

    def test_contacts_role_ansprechpartner_data(self):
        test_ds = self._build_dataset_extras_contacts(u'ansprechpartner')
        test_ds[u'maintainer'] = u'oldvalue'
        test_ds[u'maintainer_email'] = u'oldvalue@example.com'

        self._run_contacts_role_ansprechpartner_with(test_ds)

    def test_contacts_role_ansprechpartner_address(self):
        self._run_address_migration_with(u'ansprechpartner', u'maintainer',
                                         self.migrations.contacts_role_ansprechpartner)

    def _run_contacts_role_ansprechpartner_with(self, test_ds):
        self.migrations.contacts_role_ansprechpartner(test_ds)

        # expect: extras, maintainer, maintainer_email
        self._assert_dataset_len(test_ds, 3)
        self.assertDictContainsSubset({u'maintainer': u'testname'}, test_ds)
        self.assertDictContainsSubset({u'maintainer_email': u'mail@me.at'},
                                      test_ds)
        # expect: unaffected, contacts, maintainer_url, maintainer_contacttype
        self._assert_extras_len(test_ds, 4)
        self._assert_contacts_keep_testdata(test_ds)
        self._assert_extras_key_value(test_ds, u'maintainer_url',
                                      u'http://example.com/')
        self._assert_extras_key_value(test_ds, u'maintainer_contacttype',
                                      u'Organization')

    def test_contacts_role_veroeffentlichende_stelle(self):
        test_ds = self._build_dataset_extras_contacts(
            u'veroeffentlichende_stelle')

        self.migrations.contacts_role_veroeffentlichende_stelle(test_ds)

        # expect 4 new fields + contacts in addition to unaffected
        self._assert_correct_extras(test_ds, 5)
        self._assert_contacts_keep_testdata(test_ds)
        self._assert_extras_key_value(test_ds, u'publisher_name',
                                      u'testname')
        self._assert_extras_key_value(test_ds, u'publisher_email',
                                      u'mail@me.at')
        self._assert_extras_key_value(test_ds, u'publisher_url',
                                      u'http://example.com/')
        self._assert_extras_key_value(test_ds, u'publisher_contacttype',
                                      u'Organization')

    def test_contacts_role_veroeffentlichende_stelle_address(self):
        self._run_address_migration_with(u'veroeffentlichende_stelle', u'publisher',
                                         self.migrations.contacts_role_veroeffentlichende_stelle)

    def test_license_id(self):
        test_datasets = [
            {
                u'license_id': u'ogd-license-key',
                u'resources': [
                    {u'id': u'res1'},
                    {u'id': u'res2'},
                    {u'id': u'res3'}
                ]
            },
            {
                u'license_id': u'dcatde-license-key',
                u'resources': [
                    {u'id': u'res1'},
                    {u'id': u'res2'},
                    {u'id': u'res3'}
                ]
            },
        ]

        # ensure that the correct DCAT license key is added to dataset and resources
        # in both cases
        for test_ds in test_datasets:
            self.migrations.license_id(test_ds)

            self._assert_dataset_len(test_ds, 2)
            self.assertDictContainsSubset({u'license_id': u'dcatde-license-key'},
                                          test_ds)
            self.assertIn(u'resources', test_ds)

            for res in test_ds[u'resources']:
                self._assert_dataset_len(res, 2)
                self.assertIn(u'id', res)
                self.assertDictContainsSubset({
                    u'license': u'dcatde-license-key'
                }, res)

    def test_license_id_skip_unknown(self):
        test_ds = {
            u'license_id': u'other-license-key',
            u'resources': [
                {u'id': u'res1'},
                {u'id': u'res2'},
                {u'id': u'res3'}
            ]
        }

        self.migrations.license_id(test_ds)

        self._assert_dataset_len(test_ds, 2)
        # ID should remain unchanged
        self.assertDictContainsSubset({u'license_id': u'other-license-key'},
                                      test_ds)
        self.assertIn(u'resources', test_ds)

        # no license should have been added for unknown value
        for res in test_ds[u'resources']:
            self._assert_dataset_len(res, 1)
            self.assertIn(u'id', res)

    def test_terms_of_use_attribution_text(self):
        test_ds = self._build_dataset_extras(
            'terms_of_use',
            "{\"attribution_text\": \"Darf zu Testzwecken genutzt werden\"}")
        test_ds[u'resources'] = [
            {u'id': u'res1'},
            {u'id': u'res2'},
            {u'id': u'res3'}
            ]

        self.migrations.terms_of_use_attribution_text(test_ds)

        # test that attribute is in all resources
        for res in test_ds[u'resources']:
            self._assert_dataset_len(res, 2)
            self.assertIn(u'id', res)
            self.assertDictContainsSubset({
                u'licenseAttributionByText': u'Darf zu Testzwecken genutzt werden'
            }, res)

        # test that attribute is removed in dataset->extras
        self.assertNotIn('terms_of_use', test_ds['extras'])

    def test_dates_role_veroeffentlicht(self):
        test_ds = self._build_dataset_extras_dates('veroeffentlicht')

        self.migrations.dates_role_veroeffentlicht(test_ds)

        self._assert_dates_keep_testdata(test_ds)
        self._assert_date_in(test_ds, 'issued')

    def test_dates_role_aktualisiert(self):
        test_ds = self._build_dataset_extras_dates('aktualisiert')

        self.migrations.dates_role_aktualisiert(test_ds)

        self._assert_dates_keep_testdata(test_ds)
        self._assert_date_in(test_ds, 'modified')

    def test_dates_role_aktualisiert_lastfield(self):
        test_ds = self._build_dataset_extras_dates('aktualisiert', False)

        self.migrations.dates_role_aktualisiert(test_ds)

        # expect: unaffected, modified
        self._assert_extras_len(test_ds, 2)
        self._assert_extras_other_unaffected(test_ds)
        self._assert_date_in(test_ds, 'modified')

    def test_language(self):
        # build test data
        test_ds = self._build_dataset_extras(u"language", u"de")
        test_ds[u'resources'] = [
            {'language': 'en'},
            {'language': 'unknown'},  # invalid
            {'language': 'de'}
        ]

        # run function to test
        self.migrations.languages(test_ds)

        # assert results
        self._assert_extras_key_value(test_ds, u'language',
                                      u'http://publications.europa.eu/resource/authority/language/DEU')

        res = test_ds[u'resources'][0]
        self.assertDictContainsSubset({
            u'language': u'http://publications.europa.eu/resource/authority/language/ENG'
        }, res)

        res = test_ds[u'resources'][1]
        self.assertDictContainsSubset({
            u'language': 'unknown'
        }, res)

        res = test_ds[u'resources'][2]
        self.assertDictContainsSubset({
            u'language': u'http://publications.europa.eu/resource/authority/language/DEU'
        }, res)
