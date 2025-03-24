#!/usr/bin/env python
# -*- coding: utf8 -*-

import re
import unittest
import rdflib
import pkg_resources
import json

from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import Namespace, RDF
from ckanext.dcat.processors import RDFParser
from ckanext.dcatde.profiles import DCATdeProfile
from ckanext.dcat.profiles import (DCAT, DCT, ADMS, LOCN, SKOS, GSP, RDFS,
                                   VCARD, FOAF, VCARD)

DCATDE = Namespace("http://dcat-ap.de/def/dcatde/")


class BaseParseTest(unittest.TestCase):
    INVALID_TAG = u'Som`E:-in.valid tagäß!;'
    INVALID_TAG_SHORT = u';;a'
    VALID_TAG = {'name': u'some-in.valid-tagäß'}

    def _default_parser_dcatde(self):
        return RDFParser(profiles=['dcatap_de'])

    def _addLanguages(self, rdf_parser, dataset_ref, subject, predicate, text):
        object_refs = [d for d in rdf_parser.g.objects(dataset_ref, subject)]
        self.assertEqual(len(object_refs), 1)
        object_ref = object_refs[0]
        rdf_parser.g.add((object_ref, predicate, Literal(text + u' (DE)', lang='de')))
        rdf_parser.g.add((object_ref, predicate, Literal(text + u' (EN)', lang='en')))

    def _add_basic_fields_with_languages(self, rdf_parser):
        dataset_refs = [d for d in rdf_parser._datasets()]
        self.assertEqual(len(dataset_refs), 1)
        dataset_ref = dataset_refs[0]
        # Dataset
        rdf_parser.g.add((dataset_ref, DCT.title, Literal(u'Naturräume Geest und Marsch (DE)', lang='de')))
        rdf_parser.g.add((dataset_ref, DCT.title, Literal(u'Naturräume Geest und Marsch (EN)', lang='en')))
        rdf_parser.g.add((dataset_ref,
                          DCT.description,
                          Literal(
                              u'Die Zuordnung des Hamburger Stadtgebietes zu den Naturräumen Geest und Marsch wird dargestellt. (DE)',
                              lang='de')))
        rdf_parser.g.add((dataset_ref,
                          DCT.description,
                          Literal(
                              u'Die Zuordnung des Hamburger Stadtgebietes zu den Naturräumen Geest und Marsch wird dargestellt. (EN)',
                              lang='en')))
        # Publisher
        self._addLanguages(rdf_parser, dataset_ref, DCT.publisher, FOAF.name,
                           u'Behörde für Umwelt und Energie (BUE), Amt für Umweltschutz')
        # ContactPoint
        self._addLanguages(rdf_parser, dataset_ref, DCAT.contactPoint, VCARD.fn, u'Herr Dr. Michael Schröder')
        # Distributions
        distribution_refs = [d for d in rdf_parser.g.objects(dataset_ref, DCAT.distribution)]
        self.assertEqual(len(distribution_refs), 2)
        for dist_ref in distribution_refs:
            description_objects = [d for d in rdf_parser.g.objects(dist_ref, DCT.description)]
            self.assertEqual(len(description_objects), 1)
            description = description_objects[0]
            number = "4"
            if u'Distribution 1' in description:
                number = "1"
            rdf_parser.g.add((dist_ref, DCT.title, Literal(u'Naturräume Geest und Marsch (DE)', lang='de')))
            rdf_parser.g.add((dist_ref, DCT.title, Literal(u'Naturräume Geest und Marsch (EN)', lang='en')))
            rdf_parser.g.add((dist_ref,
                              DCT.description,
                              Literal(u'Das ist eine deutsche Beschreibung der Distribution %s (DE)' % number,
                                      lang='de')))
            rdf_parser.g.add((dist_ref,
                              DCT.description,
                              Literal(u'Das ist eine deutsche Beschreibung der Distribution %s (EN)' % number,
                                      lang='en')))

    def _assert_tag_list(self, dataset, expected_tags):
        """ checks if the given tags are present in the dataset """
        self.assertEqual(len(dataset['tags']), len(expected_tags))

        for tag in expected_tags:
            self.assertTrue({'name': tag} in dataset['tags'])

    def _assert_extras_string(self, extras, key, expected):
        """ check if the extras field has the expected value. """
        item = _get_value_from_extras(extras, key)
        self.assertEqual(item, expected)

    def _assert_contact_info_in_dict(self, extras, field_name):
        self._assert_extras_string(extras, field_name + '_name', u'Peter Schröder ' + field_name)
        self._assert_extras_string(extras, field_name + '_email', u'peter@schroeder.de')
        self._assert_extras_string(extras, field_name + '_contacttype', u'Person')
        self._assert_extras_string(extras, field_name + '_url', u'http://www.peterschroeder.de')
        self._assert_extras_string(extras, field_name + '_type', u'http://purl.org/adms/publishertype/LocalAuthority')

    def _assert_contact_info_combined_in_dict(self, dataset, extras, field_name, name_suffix=''):
        if name_suffix:
            name_suffix = ' ' + name_suffix
        self.assertEqual(dataset.get(field_name), u'Peter Schröder' + name_suffix)
        self.assertEqual(dataset.get(field_name + '_email'), u'peter@schroeder.de')
        self._assert_extras_string(extras, field_name + '_contacttype', u'Person')
        self._assert_extras_string(extras, field_name + '_url', u'http://www.peterschroeder.de')
        self._assert_extras_string(extras, field_name + '_type', u'http://purl.org/adms/publishertype/LocalAuthority')

    def _get_max_rdf(self, item_name="metadata_max"):
        data = pkg_resources.resource_string(__name__,
                                             "resources/%s.rdf" % item_name)

        return data

    def _assert_extras_list_serialized(self, extras, key, expected):
        """ check if the extras list value matches with the expected content.
        This assumes that the extras value is serialized as string."""
        item = _get_value_from_extras(extras, key)
        content = json.loads(item)
        self.assertCountEqual(content, expected)

    def _assert_extras_dict_serialized(self, extras, key, expected):
        """ check if the extras field with the given key contains the expected dict
        serialized as JSON."""
        item = _get_value_from_extras(extras, key)
        content = json.loads(item)
        self.assertDictEqual(content, expected)

    def _build_and_parse_format_mediatype_graph(self, format_item=None, mediatype_item=None):
        g = Graph()

        dataset = URIRef("http://example.org/datasets/1")
        g.add((dataset, RDF.type, DCAT.Dataset))

        distribution = URIRef("http://example.org/datasets/1/ds/1")
        g.add((dataset, DCAT.distribution, distribution))
        g.add((distribution, RDF.type, DCAT.Distribution))
        if format_item:
            g.add((distribution, DCT['format'], format_item))
        if mediatype_item:
            g.add((distribution, DCAT.mediaType, mediatype_item))
        if format_item is None and mediatype_item is None:
            raise AssertionError('At least one of format or mediaType is required!')

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        return dataset.get('resources')

    def _create_contact_node(self, g, contact_type):
        contact_ref = URIRef("http://example.org/datasets/1/" + contact_type)
        g.add((contact_ref, RDF.type, FOAF.Agent))
        g.add((contact_ref, FOAF.name, Literal(contact_type + u' name')))
        g.add((contact_ref, DCT.type, URIRef("http://purl.org/adms/publishertype/LocalAuthority")))
        return contact_ref

    def _assert_contact_dict(self, dataset, graph_property_name, dict_property_prefix, extras_only=False):
        extras_dict = dataset.get('extras')
        self.assertIsNotNone(extras_dict)
        if extras_only:
            self._assert_extras_string(
                extras_dict,
                dict_property_prefix + '_name', graph_property_name + u' name')
        else:
            self.assertEqual(graph_property_name + u' name', dataset.get(dict_property_prefix))

        self._assert_extras_string(
            extras_dict,
            dict_property_prefix + '_type', u'http://purl.org/adms/publishertype/LocalAuthority')
        if graph_property_name != 'publisher':
            self._assert_extras_string(
                extras_dict,
                dict_property_prefix + '_contacttype', u'Organization')

    def _add_vcard_property_with_hasvalue(self, g, contact_point, predicate, value):
        obj = BNode()
        g.add((obj, VCARD.hasValue, value))
        g.add((contact_point, predicate, obj))

    def _assert_resource_lang(self, dataset, lang_string):
        resources = dataset.get('resources')
        self.assertEqual(len(resources), 2)
        for res in resources:
            number = "4"
            if u'Distribution 1' in res.get('description'):
                number = "1"
            # Title and description to be in default language "de"
            self.assertEqual(res.get('name'), u'Naturräume Geest und Marsch (%s)' % lang_string)
            self.assertEqual(
                res.get('description'),
                u'Das ist eine deutsche Beschreibung der Distribution %s (%s)' % (number, lang_string))


class BaseSerializeTest(unittest.TestCase):
    predicate_pattern = re.compile("[a-zA-Z]:[a-zA-Z]")
    DCAT_THEME_PREFIX = "http://publications.europa.eu/resource/authority/data-theme/"

    def _get_default_dataset_dict(self):

        return {
            "id": "dct:identifier",
            "notes": "dct:description",
            "title": "dct:title",

            "url": "dcat:landingPage",
            "version": "owl:versionInfo",
            "metadata_created": "2017-07-06T13:08:40",
            "metadata_modified": "2017-07-06T13:08:41",
            "license_id": "nocheck",

            "groups": [{"name": "GRUPPEA"},
                       {"name": "GRUPPEB"}
                       ],
            "tags": [{"name": "dcat:keyword"},
                     {"name": "tagB"}
                     ],

            "author": "nocheck",
            "author_email": "nocheck",

            "maintainer": "nocheck",
            "maintainer_email": "nocheck",

            "extras": self._transform_to_key_value({
                "contributorID": ["dcatde:contributorID"],
                "qualityProcessURI": "dcatde:qualityProcessURI",
                "documentation": "foaf:page",
                "frequency": "dct:accrualPeriodicity",
                "version_notes": "adms:versionNotes",
                "dcat_type": "dct:type",
                "granularity": "dcat:granularity",
                "availability": "http://publications.europa.eu/resource/authority/planned-availability/AVAILABLE",
                "references": ["https://musterdatenkatalog.de/def/musterdatensatz/abfallwirtschaft/abfallkalender",
                               "test_references_literal"],

                "author_url": "nocheck",
                "author_type": "nocheck",

                "maintainer_url": "nocheck",
                "maintainer_tel": "nocheck",
                'maintainer_street': "nocheck",
                'maintainer_city': "nocheck",
                'maintainer_zip': "nocheck",
                'maintainer_country': "nocheck",
                "maintainer_type": "nocheck",
                "maintainer_contacttype": "nocheck",

                "contact_uri": "nocheck",
                "contact_name": "nocheck",
                "contact_email": "nocheck",
                "contact_url": "nocheck",
                "contact_tel": "nocheck",
                'contact_street': "nocheck",
                'contact_city': "nocheck",
                'contact_zip': "nocheck",
                'contact_country': "nocheck",

                "publisher_name": "nocheck",
                "publisher_email": "nocheck",
                "publisher_url": "nocheck",
                "publisher_type": "nocheck",

                "originator_name": "nocheck",
                "originator_email": "nocheck",
                "originator_url": "nocheck",
                "originator_type": "nocheck",

                "contributor_name": "nocheck",
                "contributor_email": "nocheck",
                "contributor_url": "nocheck",
                "contributor_type": "nocheck",

                "access_rights": "dct:accessRights",
                "provenance": "dct:provenance",
                "politicalGeocodingLevelURI": "dcatde:politicalGeocodingLevelURI",
                "politicalGeocodingURI": ["dcatde:politicalGeocodingURI"],
                "geocodingText": ["dcatde:geocodingDescription"],
                "legalbasisText": ["dcatde:legalBasis"],

                "temporal_start": "2017-07-06T13:08:40",
                "temporal_end": "2017-07-06T13:08:41",

                "spatial": "{\"type\":\"Polygon\",\"coordinates\":[[[8.852920532226562," +
                           "47.97245599240245],[9.133758544921875,47.97245599240245]," +
                           "[9.133758544921875,48.17249666038475],[8.852920532226562," +
                           "48.17249666038475],[8.852920532226562,47.97245599240245]]]}",

                "language": ["dct:language"],
                "conforms_to": ["dct:conformsTo"],
                "alternate_identifier": ["adms:identifier"],
                "used_datasets": ["dct:relation", "bla"],
                "has_version": ["dct:hasVersion"],
                "is_version_of": ["dct:isVersionOf"]
            }),
            "resources": [{
                "id": "id",
                "name": "dct:title",
                "description": "dct:description",
                "url": "dcat:accessURL",
                "format:": "dct:format",
                "mimetype": "dcat:mediaType",
                "size": 10,
                "hash": 24,
                "access_services": json.dumps([
                    {
                        "access_service_ref": "N4344f368651b4ec884b83dc09aad9f19",
                        "title": "Sparql-end Point 1",
                        "licenseAttributionByText": "License text 1"
                    },
                    {
                        "access_service_ref": "N4344f368651b4ec884b83dc09aad9f20",
                        "title": "Sparql-end Point 2",
                        "licenseAttributionByText": "License text 2"
                    }
                ]),

                "extras": self._transform_to_key_value({
                    "issued": "dct:issued",
                    "modified": "dct:modified",
                    "documentation": "foaf:page",
                    "download_url": "dcat:downloadURL",
                    "plannedAvailability": "dcatde:plannedAvailability",
                    "licenseAttributionByText": "dcatde:licenseAttributionByText",

                    "license": "dct:license",
                    "rights": "dct:rights",
                    "status": "adms:status",

                    "language": ["dct:language"],
                    "conforms_to": ["dct:conformsTo"],
                })
            }]
        }

    def _assert_list(self, ref, predicate, values):
        """ check for every item of a predicate to exist in the graph """
        if not isinstance(values, list):
            raise TypeError('values must be type of list')
        values_found = []
        for obj in self.graph.objects(ref, predicate):
            if str(obj) in values:
                values_found.append(str(obj))

        self.assertTrue(len(values_found) == len(values),
                        "Not all expected values were found in graph. remaining: {}".format(
                            str.join(', ', list(set(values) - set(values_found)))))

    def _predicate_from_string(self, predicate):
        """ take "dct:title" and transform to DCT.title, to be read by rdflib """
        prefix, name = predicate.split(":")
        return self.namespaces[prefix][name]

    def _check_simple_items(self, source, ref, item):
        """ checks the subgraph for different types of items """
        if isinstance(item, dict):  # handle extra-array items
            value = item["value"]
        else:
            value = source[item]

        if isinstance(value, str) and self.predicate_pattern.match(value):
            self._assert_list(ref, self._predicate_from_string(value), [value])

    def _assert_contact_info(self, dataset_ref, predicate):
        """ check name, email and url for a given rdf-subelement """
        contact = list(self.graph.objects(dataset_ref, predicate))[0]
        self.assertEqual(len(list(self.graph.objects(contact, FOAF.name))), 1,
                         predicate + " name not found")
        self.assertEqual(len(list(self.graph.objects(contact, FOAF.mbox))), 1,
                         predicate + " mbox not found")
        self.assertEqual(len(list(self.graph.objects(contact, FOAF.homepage))), 1,
                         predicate + " homepage not found")
        self.assertEqual(len(list(self.graph.objects(contact, DCT.type))), 1,
                         predicate + " dct:type not found")

    def _assert_contact_point(self, dataset_ref, remove_attr=[]):
        contact_point = next(self.graph.objects(dataset_ref, DCAT.contactPoint))
        vcard_attrs = [
            VCARD.fn, VCARD.hasEmail, VCARD.hasURL,
            VCARD.hasTelephone, VCARD.hasStreetAddress,
            VCARD.hasLocality, VCARD.hasCountryName,
            VCARD.hasPostalCode
        ]
        # remove unexpected values
        for v_attr in remove_attr:
            vcard_attrs.remove(v_attr)
        # assert expected values
        for v_attr in vcard_attrs:
            self.assertEqual(len(list(self.graph.objects(contact_point, v_attr))), 1,
                             DCAT.contactPoint + str(v_attr) + " not found")
        # assert unexpected values
        for v_attr in remove_attr:
            self.assertEqual(len(list(self.graph.objects(contact_point, v_attr))), 0,
                             DCAT.contactPoint + str(v_attr) + " found")

    def _build_graph_and_check_format_mediatype(self, dataset_dict, expected_format, expected_mediatype):
        """
        Creates a graph based on the given dict and checks for dct:format and dct:mediaType in the
        first resource element.

        :param dataset_dict:
            dataset dict, expected to contain one resource
        :param expected_format:
            expected list of dct:format items in the resource
        :param expected_mediatype:
            expected list of dcat:mediaType items in the resource
        """
        self.graph = rdflib.Graph()
        dataset_ref = URIRef("http://example.org/datasets/1")

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        # graph should contain the expected nodes
        resource_ref = list(self.graph.objects(dataset_ref, DCAT.distribution))[0]
        dct_format = list(self.graph.objects(resource_ref, DCT['format']))
        dcat_mediatype = list(self.graph.objects(resource_ref, DCAT.mediaType))
        self.assertEqual(expected_format, dct_format)
        self.assertEqual(expected_mediatype, dcat_mediatype)

    def _transform_to_key_value(self, source):
        """ convert dictionary entry to ckan-extras-field-format """
        return [{"key": key, "value": source[key]} for key in source]

    def _run_graph_from_dataset_granularity(self, _type, value):
        ### prepare ###
        dataset_dict = {
            "id": "dct:identifier",
            "notes": "dct:description",
            "title": "dct:title",
            "extras": self._transform_to_key_value({
                "granularity": value}),
            "groups": [],
            "tags": []
        }

        # execute
        self.graph = rdflib.Graph()
        dataset_ref = URIRef("http://testuri/")

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        # assert
        object_list = [x for x in self.graph.objects(dataset_ref, DCAT.granularity)]
        self.assertEqual(len(object_list), 1)
        obj = _type(value)
        self.assertEqual(obj, object_list[0], '{!r} not found in {}'.format(obj, object_list))

    def _get_dict_from_list(self, dict_list, key, value):
        """ returns the dict with the given key-value """
        for dict in dict_list:
            if (dict.get(key) == value):
                return dict
        return None

    def _assert_simple_value(self, object, predicate, value):
        obj_list = [x for x in self.graph.objects(object, predicate)]
        self.assertEqual(len(obj_list), 1, "{} not found.".format(predicate))
        self.assertTrue(obj_list[0] == value,
                        '{!r} not found in {}'.format(value, obj_list))


def _get_value_from_extras(extras, key):
    """ retrieves a value from the key-value representation used in extras dict """
    extra_value = [x["value"] for x in extras if x["key"] == key]
    if len(extra_value) > 0:
        return extra_value[0]

    return None
