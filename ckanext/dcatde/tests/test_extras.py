#!/usr/bin/python
# -*- coding: utf8 -*-
import unittest
from ckanext.dcatde.extras import Extras
from nose.tools import raises


class TestExtras(unittest.TestCase):

    def test_extras_len_works_as_expected(self):
        extras = Extras([])
        self.assertEquals(0, extras.len())

        extras = Extras({'dates': 'foo', 'bar': 'baz'})
        self.assertEquals(2, extras.len())

        extras = Extras([
            {'key': 'one', 'value': [1]},
            {'key': 'two', 'value': [2]},
            {'key': 'three', 'value': [3]},
        ])
        self.assertEquals(3, extras.len())

    def test_returns_false_on_empty_extras(self):
        extras = Extras([])
        self.assertFalse(extras.key('foo'))

    def test_returns_true_on_flat_list(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz'})
        self.assertTrue(extras.key('bar'))

    def test_returns_false_on_flat_list(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz'})
        self.assertFalse(extras.key('foo'))

    def test_returns_false_on_flat_list_with_empty_or_none_value(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz', 'foo': '', 'fuz': None})
        self.assertFalse(extras.key('foo', disallow_empty=True))
        self.assertFalse(extras.key('fuz', disallow_empty=True))

    def test_returns_true_on_dict_list(self):
        extras = Extras([
            {'key': 'foo', 'value': 'foo-value'},
            {'key': 'bar', 'value': 'bar-value'},
        ])
        self.assertTrue(extras.key('bar'))

    def test_returns_false_on_dict_list(self):
        extras = Extras([
            {'key': 'bar', 'value': 'bar-value'},
            {'key': 'baz', 'value': 'baz-value'},
        ])
        self.assertFalse(extras.key('foo'))

    def test_returns_false_on_dict_list_with_empty_or_none_value(self):
        extras = Extras([
            {'key': 'bar', 'value': 'bar-value'},
            {'key': 'baz', 'value': 'baz-value'},
            {'key': 'foo', 'value': ''},
            {'key': 'fuz', 'value': '             '},
            {'key': 'muz', 'value': None},
        ])
        self.assertFalse(extras.key('foo', disallow_empty=True))
        self.assertFalse(extras.key('fuz', disallow_empty=True))
        self.assertFalse(extras.key('muz', disallow_empty=True))

    def test_key_on_large_dict_list(self):
        extras_in = [{
            "key": "contacts",
            "value": "[{'url': 'www.open.nrw.de', 'role': 'vertrieb', 'name': 'Gesch\\u00e4ftsstelle Open.NRW', 'email': 'kontakt@open.nrw.de'}]"
        },
        {
            "key": "dates",
            "value": "[{'date': '2016-06-08T12:31:11+02:00', 'role': 'erstellt'}, {'date': '2014-05-26T12:39:03+02:00', 'role': 'veroeffentlicht'}, {'date': '2016-06-08T12:31:11+02:00', 'role': 'aktualisiert'}]"
        },
        {
            "key": "images",
            "value": "['https://open.nrw/profiles/nrw_ressort/themes/custom/nrw_base/images/grayish-blue/files/koeln_klein.png']"
        },
        {
            "key": "metadata_original_portal",
            "value": "http://open.nrw/"
        },
        {
            "key": "metadata_transformer",
            "value": ""
        },
        {
            "key": "non_open",
            "value": "false"
        },
        {
            "key": "opennrw_spatial",
            "value": "Stadt Köln"
        },
        {
            "key": "original_groups",
            "value": "['Politik und Wahlen']"
        },
        {
            "key": "spatial",
            "value": "{'type': 'polygon', 'coordinates': [[[6.7838099999999999, 50.825465999999999], [7.1533170000000004, 50.825465999999999], [7.1533170000000004, 51.090167999999998], [6.7838099999999999, 51.090167999999998], [6.7838099999999999, 50.825465999999999]]]}"
        }]

        extras = Extras(extras_in)

        for extra in extras_in:
            self.assertTrue(extras.key(extra['key']))

    @raises(KeyError)
    def test_raises_error_when_list_empty(self):
        extras = Extras([])
        extras.value('raiser')

    def test_returns_value_on_flat_list(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz'})
        self.assertEquals('foo', extras.value('dates'))

    def test_returns_expected_values(self):
        extras_in = [{
            "key": "contacts",
            "value": "[{'url': 'www.open.nrw.de', 'role': 'vertrieb', 'name': 'Gesch\\u00e4ftsstelle Open.NRW', 'email': 'kontakt@open.nrw.de'}]"
        },
        {
            "key": "dates",
            "value": "[{'date': '2016-06-08T12:31:11+02:00', 'role': 'erstellt'}, {'date': '2014-05-26T12:39:03+02:00', 'role': 'veroeffentlicht'}, {'date': '2016-06-08T12:31:11+02:00', 'role': 'aktualisiert'}]"
        },
        {
            "key": "images",
            "value": "['https://open.nrw/profiles/nrw_ressort/themes/custom/nrw_base/images/grayish-blue/files/koeln_klein.png']"
        },
        {
            "key": "metadata_original_portal",
            "value": "http://open.nrw/"
        },
        {
            "key": "metadata_transformer",
            "value": "boo"
        },
        {
            "key": "non_open",
            "value": "false"
        },
        {
            "key": "opennrw_spatial",
            "value": "Stadt Köln"
        },
        {
            "key": "original_groups",
            "value": "['Politik und Wahlen']"
        },
        {
            "key": "spatial",
            "value": "{'type': 'polygon', 'coordinates': [[[6.7838099999999999, 50.825465999999999], [7.1533170000000004, 50.825465999999999], [7.1533170000000004, 51.090167999999998], [6.7838099999999999, 51.090167999999998], [6.7838099999999999, 50.825465999999999]]]}"
        }]

        extras = Extras(extras_in)

        for extra in extras_in:
            self.assertEquals(extra['value'], extras.value(extra['key']))

    def test_returns_value_on_flat_list_with_dict(self):
        extras = Extras({
            'terms_of_use': {
                'license_id': 'some-license'
            }
        })
        self.assertEquals(
            {'license_id': 'some-license'},
            extras.value('terms_of_use')
        )

    def test_returns_default_on_flat_list(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz'})
        self.assertEquals('Default', extras.value('foo', 'Default'))

    @raises(KeyError)
    def test_raises_error_when_key_not_found_on_flat_list(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz'})
        extras.value('raiser')

    def test_returns_value_on_dict_list(self):
        extras = Extras([
            {'key': 'foo', 'value': 'foo-value'},
            {'key': 'baz', 'value': 'baz-value'},
        ])
        self.assertEquals('foo-value', extras.value('foo'))

    def test_returns_value_on_dict_list_nested(self):
        extras = Extras([
            {'key': 'foo', 'value': {
                'nested': 'nested-value',
                'zoo': 'zoo-value',
            }},
            {'key': 'baz', 'value': 'baz-value'},
        ])

        expected_value = {
            'nested': 'nested-value',
            'zoo': 'zoo-value',
        }

        self.assertEquals(expected_value, extras.value('foo'))

    def test_returns_default_on_dict_list(self):
        extras = Extras([
            {'key': 'foo', 'value': 'foo-value'},
            {'key': 'bar', 'value': 'baz'},
        ])
        self.assertEquals('OhNo', extras.value('baz', 'OhNo'))

    @raises(KeyError)
    def test_raises_error_when_key_not_found_on_dict_list(self):
        extras = Extras([{'dates': 'foo'}, {'bar': 'baz'}])
        extras.value('raiser')

    @raises(KeyError)
    def test_raises_error_when_key_not_found_for_update(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz'})
        extras.update('raiser', 'foo')

    def test_update_on_flat_list_works_as_expected(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz', 'some': 'thing'})
        self.assertTrue(extras.update('some', 'one'))
        self.assertEquals('one', extras.value('some'))

    def test_update_on_dict_list_works_as_expected(self):
        extras = Extras([
            {'key': 'hash', 'value': 'tag'},
            {'key': 'label', 'value': 'dot'},
        ])
        self.assertTrue(extras.update('label', 'doubledot'))
        self.assertEquals('doubledot', extras.value('label'))

    def test_original_groups_are_updated_as_expected(self):
        extras_in = [{
            "key": "contacts",
            "value": "[{'url': 'www.open.nrw.de', 'role': 'vertrieb', 'name': 'Gesch\\u00e4ftsstelle Open.NRW', 'email': 'kontakt@open.nrw.de'}]"
        },
        {
            "key": "dates",
            "value": "[{'date': '2016-06-08T12:31:11+02:00', 'role': 'erstellt'}, {'date': '2014-05-26T12:39:03+02:00', 'role': 'veroeffentlicht'}, {'date': '2016-06-08T12:31:11+02:00', 'role': 'aktualisiert'}]"
        },
        {
            "key": "images",
            "value": "['https://open.nrw/profiles/nrw_ressort/themes/custom/nrw_base/images/grayish-blue/files/koeln_klein.png']"
        },
        {
            "key": "metadata_original_portal",
            "value": "http://open.nrw/"
        },
        {
            "key": "metadata_transformer",
            "value": "boo"
        },
        {
            "key": "non_open",
            "value": "false"
        },
        {
            "key": "opennrw_spatial",
            "value": "Stadt Köln"
        },
        {
            "key": "original_groups",
            "value": "['Politik und Wahlen']"
        },
        {
            "key": "spatial",
            "value": "{'type': 'polygon', 'coordinates': [[[6.7838099999999999, 50.825465999999999], [7.1533170000000004, 50.825465999999999], [7.1533170000000004, 51.090167999999998], [6.7838099999999999, 51.090167999999998], [6.7838099999999999, 50.825465999999999]]]}"
        }]

        extras = Extras(extras_in)

        self.assertTrue(extras.update('original_groups', ['group one', 'group two']))
        self.assertEquals(2, len(extras.value('original_groups')))

    def upsert_on_flat_list_works_as_expected(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz', 'some': 'thing'})
        self.assertTrue(extras.update('new', 'kid', True))
        self.assertEquals('kid', extras.value('new'))
        self.assertEquals(4, extras.len())

    def upsert_on_dict_list_works_as_expected(self):
        extras = Extras([
            {'key': 'one', 'value': 1},
            {'key': 'two', 'value': 2},
        ])
        self.assertTrue(extras.update('three', 3, True))
        self.assertEquals(3, extras.value('three'))
        self.assertEquals(3, extras.len())

    def upsert_on_empty_dict_list_works_as_expected(self):
        extras = Extras()

        expected_extras = [{
            'key': 'three',
            'value': 3,
        }]

        self.assertTrue(extras.update('three', 3, True))
        self.assertEquals(3, extras.value('three'))
        self.assertEquals(1, extras.len())

        self.assertEquals(expected_extras, extras.get())

    def test_alternates_structure_as_expected(self):
        extras = Extras([
            {'key': 'terms_of_use', 'value': [{
                'licence_id': 'some-id',
                'licence_url': 'some-url',
            }]},
        ])

        expected_value = [{
            'license_id': 'some-id',
            'license_url': 'some-url',
        }]

        extras.update(
            'terms_of_use',
            expected_value,
        )

        self.assertEquals(expected_value, extras.value('terms_of_use'))
        self.assertEquals(1, len(extras.value('terms_of_use')))

    def test_returns_modified_extras(self):
        extras = Extras([
            {'key': 'terms_of_use', 'value': [{
                'license_id': 'some-id',
                'license_url': 'some-url',
            }]},
        ])

        expected_value = [{
            'license_id': 'license-id',
            'license_url': 'license-url',
            'license_type': 'license-mit',
        }]

        extras.update(
            'terms_of_use',
            expected_value,
        )

        expected_extras = [
            {'key': 'terms_of_use', 'value': [{
                'license_id': 'license-id',
                'license_url': 'license-url',
                'license_type': 'license-mit',
            }]},
        ]

        self.assertEquals(expected_extras, extras.get())

    def returns_modified_sector(self):
        extras = Extras([
            {'key': 'metadata_original_portal', 'value': None},
            {'key': 'sector', 'value': None},
        ])

        self.assertTrue(extras.update('sector', 'privat'))
        self.assertEquals('privat', extras.value('sector'))

    def test_removes_on_flat_list(self):
        extras = Extras({'dates': 'foo', 'bar': 'baz', 'some': 'thing'})
        self.assertTrue(extras.remove('bar'))
        self.assertEquals(2, extras.len())

    def test_removes_on_dict_list(self):
        extras = Extras([
            {'key': 'one', 'value': 1},
            {'key': 'two', 'value': 2},
        ])
        self.assertTrue(extras.remove('two'))
        self.assertEquals(1, extras.len())
