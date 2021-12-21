#!/usr/bin/env python
# -*- coding: utf8 -*-
""" DCAT-AP.de Profile """

import re
import unittest
import rdflib
import json
import pprint
import pkg_resources

from ckantoolkit.tests import helpers
from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import Namespace, RDF

from ckanext.dcat.profiles import EuropeanDCATAPProfile
from ckanext.dcat.processors import RDFParser
from ckanext.dcat.utils import DCAT_CLEAN_TAGS
from ckanext.dcatde.profiles import DCATdeProfile

class TestDCATde(unittest.TestCase):
    """ Test CKAN -> DCAT-AP.de export """

    # copied from ckanext.dcat.profiles
    DCT = Namespace("http://purl.org/dc/terms/")
    DCAT = Namespace("http://www.w3.org/ns/dcat#")
    ADMS = Namespace("http://www.w3.org/ns/adms#")
    VCARD = Namespace("http://www.w3.org/2006/vcard/ns#")
    FOAF = Namespace("http://xmlns.com/foaf/0.1/")
    SCHEMA = Namespace('http://schema.org/')
    TIME = Namespace('http://www.w3.org/2006/time')
    LOCN = Namespace('http://www.w3.org/ns/locn#')
    GSP = Namespace('http://www.opengis.net/ont/geosparql#')
    OWL = Namespace('http://www.w3.org/2002/07/owl#')
    SPDX = Namespace('http://spdx.org/rdf/terms#')

    # own namespace
    DCATDE = Namespace("http://dcat-ap.de/def/dcatde/")

    INVALID_TAG = u'Som`E:-in.valid tagäß!;'
    VALID_TAG = {'name': u'some-in.valid-tag'}

    dcat_theme_prefix = "http://publications.europa.eu/resource/authority/data-theme/"

    namespaces = {
        # copied from ckanext.dcat.profiles
        'dct': DCT,
        'dcat': DCAT,
        'adms': ADMS,
        'vcard': VCARD,
        'foaf': FOAF,
        'schema': SCHEMA,
        'time': TIME,
        'locn': LOCN,
        'gsp': GSP,
        'owl': OWL,
        'spdx': SPDX,

        # own extension
        'dcatde': DCATDE
    }

    predicate_pattern = re.compile("[a-zA-Z]:[a-zA-Z]")

    def _default_parser_dcatde(self):
        return RDFParser(profiles=['euro_dcat_ap', 'dcatap_de'])

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
        rdf_parser.g.add((dataset_ref, self.DCT.title, Literal(u'Naturräume Geest und Marsch (DE)', lang='de')))
        rdf_parser.g.add((dataset_ref, self.DCT.title, Literal(u'Naturräume Geest und Marsch (EN)', lang='en')))
        rdf_parser.g.add((dataset_ref,
                          self.DCT.description,
                          Literal(u'Die Zuordnung des Hamburger Stadtgebietes zu den Naturräumen Geest und Marsch wird dargestellt. (DE)', lang='de')))
        rdf_parser.g.add((dataset_ref,
                          self.DCT.description,
                          Literal(u'Die Zuordnung des Hamburger Stadtgebietes zu den Naturräumen Geest und Marsch wird dargestellt. (EN)', lang='en')))
        # Publisher
        self._addLanguages(rdf_parser, dataset_ref, self.DCT.publisher, self.FOAF.name, u'Behörde für Umwelt und Energie (BUE), Amt für Umweltschutz')
        # ContactPoint
        self._addLanguages(rdf_parser, dataset_ref, self.DCAT.contactPoint, self.VCARD.fn, u'Herr Dr. Michael Schröder')
        # Distributions
        distribution_refs = [d for d in rdf_parser.g.objects(dataset_ref, self.DCAT.distribution)]
        self.assertEqual(len(distribution_refs), 2)
        for dist_ref in distribution_refs:
            description_objects = [d for d in rdf_parser.g.objects(dist_ref, self.DCT.description)]
            self.assertEqual(len(description_objects), 1)
            description = description_objects[0]
            number = "4"
            if u'Distribution 1' in description:
                number = "1"
            rdf_parser.g.add((dist_ref, self.DCT.title, Literal(u'Naturräume Geest und Marsch (DE)', lang='de')))
            rdf_parser.g.add((dist_ref, self.DCT.title, Literal(u'Naturräume Geest und Marsch (EN)', lang='en')))
            rdf_parser.g.add((dist_ref,
                              self.DCT.description,
                              Literal(u'Das ist eine deutsche Beschreibung der Distribution %s (DE)' % number, lang='de')))
            rdf_parser.g.add((dist_ref,
                              self.DCT.description,
                              Literal(u'Das ist eine deutsche Beschreibung der Distribution %s (EN)' % number, lang='en')))

    def _transform_to_key_value(self, source):
        """ convert dictionary entry to ckan-extras-field-format """
        return [{"key": key, "value": source[key]} for key in source]

    def _get_value_from_extras(self, extras, key):
        """ retrieves a value from the key-value representation used in extras dict """
        return [x["value"] for x in extras if x["key"] == key][0]

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

    def _assert_extras_list_serialized(self, extras, key, expected):
        """ check if the extras list value matches with the expected content.
        This assumes that the extras value is serialized as string."""
        item = self._get_value_from_extras(extras, key)
        content = json.loads(item)
        self.assertCountEqual(content, expected)

    def _assert_extras_dict_serialized(self, extras, key, expected):
        """ check if the extras field with the given key contains the expected dict
        serialized as JSON."""
        item = self._get_value_from_extras(extras, key)
        content = json.loads(item)
        self.assertDictEqual(content, expected)

    def _assert_extras_string(self, extras, key, expected):
        """ check if the extras field has the expected value. """
        item = self._get_value_from_extras(extras, key)
        self.assertEqual(item, expected)

    def _assert_tag_list(self, dataset, expected_tags):
        """ checks if the given tags are present in the dataset """
        self.assertEqual(len(dataset['tags']), len(expected_tags))

        for tag in expected_tags:
            self.assertTrue({'name': tag} in dataset['tags'])

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

    def _assert_contact_info_in_dict(self, extras, field_name):
        self._assert_extras_string(extras, field_name + '_name', u'Peter Schröder ' + field_name)
        self._assert_extras_string(extras, field_name + '_contacttype', u'Person')
        self._assert_extras_string(extras, field_name + '_type', u'http://purl.org/adms/publishertype/LocalAuthority')

    def _assert_contact_info_combined_in_dict(self, dataset, extras, field_name, name_suffix=''):
        if name_suffix:
            name_suffix = ' ' + name_suffix
        self.assertEqual(dataset.get(field_name), u'Peter Schröder' + name_suffix)
        self._assert_extras_string(extras, field_name + '_contacttype', u'Person')
        self._assert_extras_string(extras, field_name + '_type', u'http://purl.org/adms/publishertype/LocalAuthority')

    def _assert_contact_info(self, dataset_ref, predicate):
        """ check name, email and url for a given rdf-subelement """
        contact = list(self.graph.objects(dataset_ref, predicate))[0]
        self.assertEqual(len(list(self.graph.objects(contact, self.FOAF.name))), 1,
                         predicate + " name not found")
        self.assertEqual(len(list(self.graph.objects(contact, self.FOAF.mbox))), 1,
                         predicate + " mbox not found")
        self.assertEqual(len(list(self.graph.objects(contact, self.FOAF.homepage))), 1,
                         predicate + " homepage not found")
        self.assertEqual(len(list(self.graph.objects(contact, self.DCT.type))), 1,
                         predicate + " dct:type not found")

    def _assert_contact_point(self, dataset_ref, remove_attr=[]):
        contact_point = next(self.graph.objects(dataset_ref, self.DCAT.contactPoint))
        vcard_attrs = [
            self.VCARD.fn, self.VCARD.hasEmail, self.VCARD.hasURL,
            self.VCARD.hasTelephone, self.VCARD.hasStreetAddress,
            self.VCARD.hasLocality, self.VCARD.hasCountryName,
            self.VCARD.hasPostalCode
        ]
        # remove unexpected values
        for v_attr in remove_attr:
            vcard_attrs.remove(v_attr)
        # assert expected values
        for v_attr in vcard_attrs:
            self.assertEqual(len(list(self.graph.objects(contact_point, v_attr))), 1,
                             self.DCAT.contactPoint + str(v_attr) + " not found")
        # assert unexpected values
        for v_attr in remove_attr:
            self.assertEqual(len(list(self.graph.objects(contact_point, v_attr))), 0,
                             self.DCAT.contactPoint + str(v_attr) + " found")

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

    def _get_max_rdf(self, item_name="metadata_max"):
        data = pkg_resources.resource_string(__name__,
                                             "resources/%s.rdf" % item_name)

        return data

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
            "tags": [{"name":"dcat:keyword"},
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

                "author_url": "nocheck",
                "author_type": "nocheck",

                "maintainer_url": "nocheck",
                "maintainer_tel": "nocheck",
                'maintainer_street': "nocheck",
                'maintainer_city': "nocheck",
                'maintainer_zip': "nocheck",
                'maintainer_country': "nocheck",
                "maintainer_type": "nocheck",

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
                "alternate_identifier" : ["adms:identifier"],
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

    def _build_and_parse_format_mediatype_graph(self, format_item=None, mediatype_item=None):
        g = Graph()

        dataset = URIRef("http://example.org/datasets/1")
        g.add((dataset, RDF.type, self.DCAT.Dataset))

        distribution = URIRef("http://example.org/datasets/1/ds/1")
        g.add((dataset, self.DCAT.distribution, distribution))
        g.add((distribution, RDF.type, self.DCAT.Distribution))
        if format_item:
            g.add((distribution, self.DCT['format'], format_item))
        if mediatype_item:
            g.add((distribution, self.DCAT.mediaType, mediatype_item))
        if format_item is None and mediatype_item is None:
            raise AssertionError('At least one of format or mediaType is required!')

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        return dataset.get('resources')

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

        dcat = EuropeanDCATAPProfile(self.graph, False)
        dcat.graph_from_dataset(dataset_dict, dataset_ref)

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        # graph should contain the expected nodes
        resource_ref = list(self.graph.objects(dataset_ref, self.DCAT.distribution))[0]
        dct_format = list(self.graph.objects(resource_ref, self.DCT['format']))
        dcat_mediatype = list(self.graph.objects(resource_ref, self.DCAT.mediaType))
        self.assertEqual(expected_format, dct_format)
        self.assertEqual(expected_mediatype, dcat_mediatype)

    def _create_contact_node(self, g, contact_type):
        contact_ref = URIRef("http://example.org/datasets/1/" + contact_type)
        g.add((contact_ref, RDF.type, self.FOAF.Agent))
        g.add((contact_ref, self.FOAF.name, Literal(contact_type + u' name')))
        g.add((contact_ref, self.DCT.type, URIRef("http://purl.org/adms/publishertype/LocalAuthority")))
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
        g.add((obj, self.VCARD.hasValue, value))
        g.add((contact_point, predicate, obj))

    def test_graph_from_dataset(self):
        """ test dcat and dcatde profiles """

        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()
        dataset_ref = URIRef("http://testuri/")

        dcat = EuropeanDCATAPProfile(self.graph, False)
        dcat.graph_from_dataset(dataset_dict, dataset_ref)

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        # Assert structure of graph - basic values
        extras = dataset_dict["extras"]

        for key in dataset_dict:
            self._check_simple_items(dataset_dict, dataset_ref, key)

        for key in extras:
            self._check_simple_items(dataset_dict, dataset_ref, key)

        # issued, modified
        self.assertEqual(len(list(self.graph.objects(dataset_ref, self.DCT.issued))), 1,
                         "dct:issued not found")
        self.assertEqual(len(list(self.graph.objects(dataset_ref, self.DCT.modified))), 1,
                         "dct:modified not found")

        # groups, tags
        self._assert_list(dataset_ref, self.DCAT.theme,
                         [self.dcat_theme_prefix + x["name"] for x in dataset_dict["groups"]])
        self._assert_list(dataset_ref, self.DCAT.keyword,
                         [x["name"] for x in dataset_dict["tags"]])

        # author, maintainer, originator, contributor, publisher
        self._assert_contact_info(dataset_ref, self.DCATDE.originator)
        self._assert_contact_info(dataset_ref, self.DCATDE.maintainer)
        self._assert_contact_info(dataset_ref, self.DCT.contributor)
        self._assert_contact_info(dataset_ref, self.DCT.creator)
        self._assert_contact_info(dataset_ref, self.DCT.publisher)

        # contactPoint
        self._assert_contact_point(dataset_ref)

        # temporal
        temporal = list(self.graph.objects(dataset_ref, self.DCT.temporal))[0]
        self.assertEqual(len(list(self.graph.objects(temporal, self.SCHEMA.startDate))), 1,
                         self.SCHEMA.startDate + " not found")
        self.assertEqual(len(list(self.graph.objects(temporal, self.SCHEMA.endDate))), 1,
                         self.SCHEMA.endDate + " not found")

        # spatial
        for spatial in list(self.graph.objects(dataset_ref, self.DCT.spatial)):
            geonodes = len(list(self.graph.objects(spatial, self.LOCN.geometry)))
            adminnodes = len(list(self.graph.objects(spatial, self.LOCN.adminUnitL2)))
            if geonodes > 0:
                self.assertEqual(geonodes, 2, self.LOCN.geometry + " not present, 2x")
            elif adminnodes > 0:
                self.assertEqual(adminnodes, 1, self.LOCN.adminUnitL2 + " not present")
            else:
                self.fail("No valid spatial blocks found.")

        # lists in extras
        self._assert_list(dataset_ref, self.DCT.language,
                         self._get_value_from_extras(extras, "language"))
        self._assert_list(dataset_ref, self.DCT.conformsTo,
                         self._get_value_from_extras(extras, "conforms_to"))
        self._assert_list(dataset_ref, self.ADMS.identifier,
                         self._get_value_from_extras(extras, "alternate_identifier"))
        self._assert_list(dataset_ref, self.DCT.relation,
                         self._get_value_from_extras(extras, "used_datasets"))
        self._assert_list(dataset_ref, self.DCT.hasVersion,
                         self._get_value_from_extras(extras, "has_version"))
        self._assert_list(dataset_ref, self.DCT.isVersionOf,
                         self._get_value_from_extras(extras, "is_version_of"))
        self._assert_list(dataset_ref, self.DCATDE.politicalGeocodingURI,
                         self._get_value_from_extras(extras, "politicalGeocodingURI"))
        self._assert_list(dataset_ref, self.DCATDE.geocodingDescription,
                         self._get_value_from_extras(extras, "geocodingText"))
        self._assert_list(dataset_ref, self.DCATDE.legalBasis,
                         self._get_value_from_extras(extras, "legalbasisText"))
        self._assert_list(dataset_ref, self.DCATDE.contributorID,
                         self._get_value_from_extras(extras, "contributorID"))

        # resources
        resource = dataset_dict["resources"][0]
        resource_ref = list(self.graph.objects(dataset_ref, self.DCAT.distribution))[0]
        resource_extras = resource["extras"]

        for key in resource:
            self._check_simple_items(resource, resource_ref, key)

        for key in resource_extras:
            self._check_simple_items(resource, resource_ref, key)

        # size
        self.assertEqual(len(list(self.graph.objects(resource_ref, self.DCAT.byteSize))), 1,
                         self.DCAT.byteSize + " not found")

        # hash
        self.assertEqual(len(list(self.graph.objects(resource_ref, self.SPDX.checksum))), 1,
                         self.SPDX.checksum + " not found")

        # lists
        self._assert_list(resource_ref, self.DCT.language,
                         self._get_value_from_extras(resource_extras, "language"))
        self._assert_list(resource_ref, self.DCT.conformsTo,
                         self._get_value_from_extras(resource_extras, "conforms_to"))

    def test_graph_from_dataset_only_dcatde_contact_point_values(self):

        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()
        # remove fields processed by ckanext-dcat default profile
        dataset_dict.pop('maintainer')
        dataset_dict.pop('maintainer_email')
        dataset_dict.pop('author')
        dataset_dict.pop('author_email')
        dataset_ref = URIRef("http://testuri/")

        ### execute ###
        dcat = EuropeanDCATAPProfile(self.graph, False)
        dcat.graph_from_dataset(dataset_dict, dataset_ref)

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        ### assert ###
        # contactPoint
        self._assert_contact_point(dataset_ref, [self.VCARD.fn, self.VCARD.hasEmail])

    def test_graph_from_dataset_format_iana_uri(self):
        dataset_dict = self._get_default_dataset_dict()
        # when only format is available and it looks like an IANA media type, use DCAT.mediaType instead
        # of DCT.format for output
        fmt_uri = 'https://www.iana.org/assignments/media-types/application/json'
        dataset_dict['resources'][0]['format'] = fmt_uri
        dataset_dict['resources'][0].pop('mimetype')

        # expect no dct:format node and the URI in dcat:mediaType
        self._build_graph_and_check_format_mediatype(
            dataset_dict,
            [],
            [URIRef(fmt_uri)]
        )

    def test_graph_from_dataset_format_other_uri(self):
        dataset_dict = self._get_default_dataset_dict()
        # when only format is available and it does not look like an IANA media type, use dct:format
        fmt_uri = 'https://example.com/my/format'
        dataset_dict['resources'][0]['format'] = fmt_uri
        dataset_dict['resources'][0].pop('mimetype')
        dataset_ref = URIRef("http://example.org/datasets/1")

        # expect dct:format node with the URI and no dcat:mediaType
        self._build_graph_and_check_format_mediatype(
            dataset_dict,
            [URIRef(fmt_uri)],
            []
        )

    def test_graph_from_dataset_format_mediatype_text(self):
        dataset_dict = self._get_default_dataset_dict()
        # if format value looks like an IANA media type, output dcat:mediaType instead of dct:format
        fmt_text = 'application/json'
        dataset_dict['resources'][0]['format'] = fmt_text
        dataset_dict['resources'][0].pop('mimetype')

        # expect no dct:format node and the literal value in dcat:mediaType
        self._build_graph_and_check_format_mediatype(
            dataset_dict,
            [],
            [Literal(fmt_text)]
        )

    def test_graph_from_dataset_format_mediatype_same(self):
        dataset_dict = self._get_default_dataset_dict()
        # if format and mediaType are identical, output only dcat:mediaType
        fmt_text = 'application/json'
        dataset_dict['resources'][0]['format'] = fmt_text
        dataset_dict['resources'][0]['mimetype'] = fmt_text

        # expect no dct:format node and the literal value in dcat:mediaType
        self._build_graph_and_check_format_mediatype(
            dataset_dict,
            [],
            [Literal(fmt_text)]
        )

    def test_graph_from_dataset_format_mediatype_different(self):
        dataset_dict = self._get_default_dataset_dict()
        # if format and mediaType are different, output both
        dataset_dict['resources'][0]['format'] = 'myformat'
        dataset_dict['resources'][0]['mimetype'] = 'application/json'

        # expect both nodes
        self._build_graph_and_check_format_mediatype(
            dataset_dict,
            [Literal('myformat')],
            [Literal('application/json')]
        )

    def test_graph_from_dataset_contributorID_uriref_or_literal(self):
        ### prepare ###
        values_in_dataset_dict = ['contributorID', 'http://dcat-ap.de/def/contributors/contributorID']
        dataset_dict = {
            "id": "dct:identifier",
            "notes": "dct:description",
            "title": "dct:title",
            "extras": self._transform_to_key_value({
                "contributorID": values_in_dataset_dict}),
            "groups": [],
            "tags": []
            }

        # execute
        self.graph = rdflib.Graph()
        dataset_ref = URIRef("http://testuri/")

        dcat = EuropeanDCATAPProfile(self.graph, False)
        dcat.graph_from_dataset(dataset_dict, dataset_ref)

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        # assert
        values = self._get_value_from_extras(dataset_dict["extras"], 'contributorID')
        self.assertEqual(len(values), len(values_in_dataset_dict))
        for item in [
            ('contributorID', self.DCATDE.contributorID, [Literal, URIRef])
        ]:
            for num, value in enumerate(values):
                _type = item[2]
                if isinstance(item[2], list):
                    self.assertEqual(len(item[2]), len(values))
                    _type = item[2][num]
                obj = _type(value)
                self.assertIn(obj, self.graph.objects(dataset_ref, item[1]),
                              '{!r} not found in {}'.format(
                                  obj, [x for x in self.graph.objects(dataset_ref, item[1])]))

    def test_graph_from_dataset_granularity_literal(self):
        self._run_graph_from_dataset_granularity(Literal, 'MONTHLY')

    def test_graph_from_dataset_granularity_uriref(self):
        self._run_graph_from_dataset_granularity(
            URIRef, 'http://publications.europa.eu/resource/authority/frequency/MONTHLY')

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

        dcat = EuropeanDCATAPProfile(self.graph, False)
        dcat.graph_from_dataset(dataset_dict, dataset_ref)

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        # assert
        object_list = [x for x in self.graph.objects(dataset_ref, self.DCAT.granularity)]
        self.assertEqual(len(object_list), 1)
        obj = _type(value)
        self.assertEqual(obj, object_list[0], '{!r} not found in {}'.format(obj, object_list))

    def test_parse_dataset_generic(self):
        self._run_parse_dataset('metadata_max', latest_version=True)

    def test_parse_dataset_v1_0_1(self):
        self._run_parse_dataset('metadata_max_1_0_1')

    def test_parse_dataset_v1_0(self):
        self._run_parse_dataset('metadata_max_1_0')

    def test_parse_dataset_multi_namespaces(self):
        self._run_parse_dataset('metadata_max_multi_namespaces')

    def _run_parse_dataset(self, max_rdf_file, latest_version=False):
        maxrdf = self._get_max_rdf(max_rdf_file)

        p = self._default_parser_dcatde()

        p.parse(maxrdf)

        datasets = [d for d in p.datasets()]
        self.assertEqual(len(datasets), 1)
        dataset = datasets[0]

        extras = dataset.get('extras')
        self.assertTrue(len(extras) > 0)
        resources = dataset.get('resources')
        self.assertEqual(len(resources), 2)

        # identify resources to be independent of their order
        if u'Distribution 1' in resources[0].get('description'):
            dist1 = resources[0]
            dist2 = resources[1]
        else:
            dist1 = resources[1]
            dist2 = resources[0]

        # list values are serialized by parser

        # dcatde:maintainer
        self._assert_contact_info_combined_in_dict(dataset, extras, 'maintainer')

        # dcatde:contributorID
        self._assert_extras_list_serialized(
            extras, 'contributorID',
            ['http://dcat-ap.de/def/contributors/transparenzportalHamburg'])

        # dcatde:originator
        self._assert_contact_info_in_dict(extras, 'originator')

        # dct:creator
        self._assert_contact_info_combined_in_dict(dataset, extras, 'author', 'creator')

        # dct:contributor
        self._assert_contact_info_in_dict(extras, 'contributor')

        if latest_version:
            # dcat:granularity
            self._assert_extras_string(extras, 'granularity',
                                      'http://publications.europa.eu/resource/authority/frequency/MONTHLY')
        else:
            self.assertEqual(len([x for x in extras if x["key"] == 'granularity']), 0)

        # dcatde:politicalGeocodingURI
        self._assert_extras_list_serialized(
            extras, 'politicalGeocodingURI',
            ['http://dcat-ap.de/def/politicalGeocoding/regionalKey/020000000000',
             'http://dcat-ap.de/def/politicalGeocoding/stateKey/02'])

        # dcatde:politicalGeocodingLevelURI
        self._assert_extras_string(extras, 'politicalGeocodingLevelURI',
                                  'http://dcat-ap.de/def/politicalGeocoding/Level/state')

        # dcatde:legalBasis
        self._assert_extras_list_serialized(extras, 'legalbasisText',
                                           ['Umweltinformationsgesetz (UIG)'])

        # dcatde:geocodingDescription
        self._assert_extras_list_serialized(extras, 'geocodingText',
                                           ['Hamburg'])

        # dcatde:qualityProcessURI
        self._assert_extras_string(extras, 'qualityProcessURI',
                                  'https://www.example.com/')

        # resource checks
        self.assertEqual(dist1['__extras'].get('plannedAvailability'),
                         'http://dcat-ap.de/def/plannedAvailability/experimental')
        self.assertEqual(dist1['__extras'].get('licenseAttributionByText'),
                         u'Freie und Hansestadt Hamburg, Behörde für Umwelt und Energie, 2016')
        self.assertEqual(dist1.get('license'),
                         "http://dcat-ap.de/def/licenses/dl-by-de/2_0")
        self.assertEqual(dist1.get('size'), 685246)

        self.assertEqual(dist2['__extras'].get('plannedAvailability'),
                         'http://dcat-ap.de/def/plannedAvailability/available')
        self.assertEqual(dist2['__extras'].get('licenseAttributionByText'),
                         u'Freie und Hansestadt Hamburg, Behörde für Umwelt und Energie, 2015')
        self.assertEqual(dist2.get('license'),
                         "http://dcat-ap.de/def/licenses/dl-by-de/2_0")
        self.assertEqual(dist2.get('size'), 222441)


        # some non-dcatde fields
        self._assert_extras_list_serialized(extras, 'alternate_identifier',
                                           ['4635D337-4805-4C32-A211-13F8C038BF27'])

        # dcat:contactPoint
        self._assert_extras_string(extras, 'contact_email', u'michael.schroeder@bue.hamburg.de')
        self._assert_extras_string(extras, 'contact_name', u'Herr Dr. Michael Schröder')
        self._assert_extras_string(extras, 'maintainer_tel', u'+49 40 4 28 40 - 3494')
        self._assert_extras_string(extras, 'maintainer_street', u'Beispielstraße 4')
        self._assert_extras_string(extras, 'maintainer_city', u'Beispielort')
        self._assert_extras_string(extras, 'maintainer_zip', u'12345')
        self._assert_extras_string(extras, 'maintainer_country', u'DE')

        # Groups
        self.assertEqual(len(dataset['groups']), 2)
        self.assertTrue({'id': 'envi', 'name': 'envi'} in dataset['groups'])
        self.assertTrue({'id': 'agri', 'name': 'agri'} in dataset['groups'])

        # Keywords
        self._assert_tag_list(
            dataset,
            [u'karte', u'hmbtg_09_geodaten', u'grundwasser', u'bodenschutz', u'geodaten',
             u'umwelt-und-klima', u'hmbtg', u'opendata', u'thematische-karte'])

        # dct:location
        self._assert_extras_dict_serialized(
            extras, 'spatial', {"type": "Polygon",
                                "coordinates": [[[10.3263, 53.3949], [10.3263, 53.9641], [8.4205, 53.9641],
                                                 [8.4205, 53.3949], [10.3263, 53.3949]]]})

        # dcat:landingPage
        self._assert_extras_string(
            extras, 'metadata_original_html',
            'https://www.govdata.de/web/guest/daten/-/details/naturraume-geest-und-marsch3')

    @helpers.change_config('ckan.locale_default', 'en')
    def test_parse_dataset_default_lang_en(self):
        maxrdf = self._get_max_rdf()

        p = self._default_parser_dcatde()

        p.parse(maxrdf)
        self._add_basic_fields_with_languages(p)

        datasets = [d for d in p.datasets()]
        self.assertEqual(len(datasets), 1)
        dataset = datasets[0]

        # Title and description to be in default language "en"
        self.assertEqual(dataset.get('title'), u'Naturräume Geest und Marsch (EN)')
        self.assertEqual(
            dataset.get('notes'),
            u'Die Zuordnung des Hamburger Stadtgebietes zu den Naturräumen Geest und Marsch wird dargestellt. (EN)')
        # Publisher and ContactPoint
        extras = dataset.get('extras')
        self.assertTrue(len(extras) > 0)
        self._assert_extras_string(extras, 'publisher_name', u'Behörde für Umwelt und Energie (BUE), Amt für Umweltschutz (EN)')
        self._assert_extras_string(extras, 'contact_name', u'Herr Dr. Michael Schröder (EN)')
        # Resources
        self._assert_resource_lang(dataset, 'EN')

    @helpers.change_config('ckan.locale_default', 'de')
    def test_parse_dataset_default_lang_de(self):
        maxrdf = self._get_max_rdf()

        p = self._default_parser_dcatde()

        p.parse(maxrdf)
        self._add_basic_fields_with_languages(p)

        datasets = [d for d in p.datasets()]
        self.assertEqual(len(datasets), 1)
        dataset = datasets[0]

        # Title and description to be in default language "de"
        self.assertEqual(dataset.get('title'), u'Naturräume Geest und Marsch (DE)')
        self.assertEqual(
            dataset.get('notes'),
            u'Die Zuordnung des Hamburger Stadtgebietes zu den Naturräumen Geest und Marsch wird dargestellt. (DE)')
        # Publisher and ContactPoint
        extras = dataset.get('extras')
        self.assertTrue(len(extras) > 0)
        self._assert_extras_string(extras, 'publisher_name', u'Behörde für Umwelt und Energie (BUE), Amt für Umweltschutz (DE)')
        self._assert_extras_string(extras, 'contact_name', u'Herr Dr. Michael Schröder (DE)')
        # Resources
        self._assert_resource_lang(dataset, 'DE')

    @helpers.change_config('ckan.locale_default', 'fr')
    def test_parse_dataset_default_lang_not_in_graph(self):
        maxrdf = self._get_max_rdf()

        p = self._default_parser_dcatde()

        p.parse(maxrdf)
        self._add_basic_fields_with_languages(p)

        datasets = [d for d in p.datasets()]
        self.assertEqual(len(datasets), 1)
        dataset = datasets[0]

        # Title and description random
        self.assertIn(u'Naturräume Geest und Marsch', dataset.get('title'))
        self.assertIn(
            u'Die Zuordnung des Hamburger Stadtgebietes zu den Naturräumen Geest und Marsch wird dargestellt',
            dataset.get('notes'))
        # Publisher and ContactPoint
        extras = dataset.get('extras')
        self.assertTrue(len(extras) > 0)
        self.assertIn(u'Behörde für Umwelt und Energie (BUE), Amt für Umweltschutz', self._get_value_from_extras(extras, 'publisher_name'))
        self.assertIn(u'Herr Dr. Michael Schröder', self._get_value_from_extras(extras, 'contact_name'))
        # Resources
        resources = dataset.get('resources')
        self.assertEqual(len(resources), 2)
        for res in resources:
            # Title and description random
            self.assertIn(u'Naturräume Geest und Marsch', res.get('name'))
            self.assertIn(
                u'Das ist eine deutsche Beschreibung der Distribution',
                res.get('description'))

    def test_parse_dataset_dct_format_iana_uri(self):
        resources = self._build_and_parse_format_mediatype_graph(
            URIRef("https://www.iana.org/assignments/media-types/application/json")
        )
        # IANA mediatype URI should be added to mimetype field as well
        self.assertIn(u'json', resources[0].get('format').lower())
        self.assertEqual(u'https://www.iana.org/assignments/media-types/application/json',
                         resources[0].get('mimetype'))

    def test_parse_dataset_mediatype_iana_uri_without_format(self):
        resources = self._build_and_parse_format_mediatype_graph(
            mediatype_item=URIRef("https://www.iana.org/assignments/media-types/application/json")
        )
        # IANA mediatype URI should be added to mimetype field and to format as well
        self.assertEqual(u'https://www.iana.org/assignments/media-types/application/json',
                         resources[0].get('mimetype'))
        self.assertEqual(u'https://www.iana.org/assignments/media-types/application/json',
                         resources[0].get('format'))

    def test_parse_dataset_dct_format_other_uri(self):
        resources = self._build_and_parse_format_mediatype_graph(
            URIRef("https://example.com/my/format")
        )
        self.assertEqual(u'https://example.com/my/format',
                         resources[0].get('format'))
        self.assertEqual(None, resources[0].get('mimetype'))

    def test_parse_dataset_dct_format_mediatype_text(self):
        resources = self._build_and_parse_format_mediatype_graph(
            Literal("application/json")
        )
        # IANA mediatype should be added to mimetype field as well
        self.assertIn(u'json', resources[0].get('format').lower())
        self.assertEqual(u'application/json',
                         resources[0].get('mimetype'))

    def test_parse_dataset_format_and_dcat_mediatype(self):
        # Even if dct:format is a valid IANA type, prefer dcat:mediaType if given
        resources = self._build_and_parse_format_mediatype_graph(
            Literal("application/json"), Literal("test-mediatype")
        )
        # both should be stored
        self.assertIn(u'json', resources[0].get('format').lower())
        self.assertEqual(u'test-mediatype',
                         resources[0].get('mimetype'))

    def test_parse_contacts_graph_type_foaf_agent(self):
        # prepare
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        # author/creator, maintainer, originator, contributor, publisher
        originator = self._create_contact_node(g, 'originator')
        g.add((dataset_ref, self.DCATDE.originator, originator))
        maintainer = self._create_contact_node(g, 'maintainer')
        g.add((dataset_ref, self.DCATDE.maintainer, maintainer))
        contributor = self._create_contact_node(g, 'contributor')
        g.add((dataset_ref, self.DCT.contributor, contributor))
        creator = self._create_contact_node(g, 'creator')
        g.add((dataset_ref, self.DCT.creator, creator))
        publisher = self._create_contact_node(g, 'publisher')
        g.add((dataset_ref, self.DCT.publisher, publisher))

        # execute
        p = self._default_parser_dcatde()
        p.g = g
        dataset = [d for d in p.datasets()][0]

        # assert
        self.assertIsNotNone(dataset)
        self._assert_contact_dict(dataset, 'originator', 'originator', True)
        self._assert_contact_dict(dataset, 'maintainer', 'maintainer')
        self._assert_contact_dict(dataset, 'contributor', 'contributor', True)
        self._assert_contact_dict(dataset, 'creator', 'author')
        self._assert_contact_dict(dataset, 'publisher', 'publisher', True)

    def test_dataset_contact_point_vcard_hasURL_hasTelephone_literal(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, self.VCARD.Organization))
        g.add((contact_point, self.VCARD.hasURL, Literal('http://contact-point-url.de')))
        g.add((contact_point, self.VCARD.hasTelephone, Literal('+490531-24262-10')))
        g.add((dataset_ref, self.DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'maintainer_url', u'http://contact-point-url.de')
        self._assert_extras_string(extras, 'maintainer_tel', u'+490531-24262-10')

    def test_dataset_contact_point_vcard_hasURL_hasTelephone_uriref(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, self.VCARD.Organization))
        g.add((contact_point, self.VCARD.hasURL, URIRef('http://contact-point-url.de')))
        g.add((contact_point, self.VCARD.hasTelephone, URIRef('tel:+490531-24262-10')))
        g.add((dataset_ref, self.DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'maintainer_url', u'http://contact-point-url.de')
        self._assert_extras_string(extras, 'maintainer_tel', u'+490531-24262-10')

    def test_dataset_contact_point_vcard_hasURL_hasTelephone_hasValue_literal(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, self.VCARD.Organization))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, self.VCARD.hasURL, Literal('http://contact-point-url.de'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, self.VCARD.hasTelephone, Literal('+490531-24262-10'))
        g.add((dataset_ref, self.DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'maintainer_url', u'http://contact-point-url.de')
        self._assert_extras_string(extras, 'maintainer_tel', u'+490531-24262-10')

    def test_dataset_contact_point_vcard_hasURL_hasTelephone_hasValue_uriref(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, self.VCARD.Organization))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, self.VCARD.hasURL, URIRef('http://contact-point-url.de'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, self.VCARD.hasTelephone, URIRef('tel:+490531-24262-10'))
        g.add((dataset_ref, self.DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'maintainer_url', u'http://contact-point-url.de')
        self._assert_extras_string(extras, 'maintainer_tel', u'+490531-24262-10')

    def test_dataset_contact_point_vcard_address_has_fields_direct(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, self.VCARD.Organization))
        g.add((contact_point, self.VCARD.hasLocality, Literal('Berlin')))
        g.add((contact_point, self.VCARD.hasStreetAddress, Literal('Hauptstraße 1')))
        g.add((contact_point, self.VCARD.hasPostalCode, Literal('12345')))
        g.add((contact_point, self.VCARD.hasCountryName, Literal('Deutschland')))
        g.add((dataset_ref, self.DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'maintainer_city', u'Berlin')
        self._assert_extras_string(extras, 'maintainer_street', u'Hauptstraße 1')
        self._assert_extras_string(extras, 'maintainer_zip', u'12345')
        self._assert_extras_string(extras, 'maintainer_country', u'Deutschland')

    def test_dataset_contact_point_vcard_address_has_fields_direct_with_hasvalue(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, self.VCARD.Organization))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, self.VCARD.hasLocality, Literal('Berlin'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, self.VCARD.hasStreetAddress, Literal('Hauptstraße 1'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, self.VCARD.hasPostalCode, Literal('12345'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, self.VCARD.hasCountryName, Literal('Deutschland'))
        g.add((dataset_ref, self.DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'maintainer_city', u'Berlin')
        self._assert_extras_string(extras, 'maintainer_street', u'Hauptstraße 1')
        self._assert_extras_string(extras, 'maintainer_zip', u'12345')
        self._assert_extras_string(extras, 'maintainer_country', u'Deutschland')

    def test_dataset_contact_point_vcard_address_has_fields_within_address_object(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, self.VCARD.Organization))
        address = BNode()
        g.add((address, RDF.type, self.VCARD.Address))
        g.add((address, self.VCARD.hasLocality, Literal('Berlin')))
        g.add((address, self.VCARD.hasStreetAddress, Literal('Hauptstraße 1')))
        g.add((address, self.VCARD.hasPostalCode, Literal('12345')))
        g.add((address, self.VCARD.hasCountryName, Literal('Deutschland')))
        g.add((contact_point, self.VCARD.hasAddress, address))
        g.add((dataset_ref, self.DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'maintainer_city', u'Berlin')
        self._assert_extras_string(extras, 'maintainer_street', u'Hauptstraße 1')
        self._assert_extras_string(extras, 'maintainer_zip', u'12345')
        self._assert_extras_string(extras, 'maintainer_country', u'Deutschland')

    def test_dataset_contact_point_vcard_address_has_fields_within_address_object_with_hasvalue(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, self.VCARD.Organization))
        address = BNode()
        g.add((address, RDF.type, self.VCARD.Address))
        self._add_vcard_property_with_hasvalue(
            g, address, self.VCARD.hasLocality, Literal('Berlin'))
        self._add_vcard_property_with_hasvalue(
            g, address, self.VCARD.hasStreetAddress, Literal('Hauptstraße 1'))
        self._add_vcard_property_with_hasvalue(
            g, address, self.VCARD.hasPostalCode, Literal('12345'))
        self._add_vcard_property_with_hasvalue(
            g, address, self.VCARD.hasCountryName, Literal('Deutschland'))
        g.add((contact_point, self.VCARD.hasAddress, address))
        g.add((dataset_ref, self.DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'maintainer_city', u'Berlin')
        self._assert_extras_string(extras, 'maintainer_street', u'Hauptstraße 1')
        self._assert_extras_string(extras, 'maintainer_zip', u'12345')
        self._assert_extras_string(extras, 'maintainer_country', u'Deutschland')

    def test_parse_dataset_remove_mailto_from_email(self):
        g = Graph()

        maintainer_email = 'demo.maintainer@org.de'
        maintainer = BNode()
        g.add((maintainer, RDF.type, self.FOAF.Organization))
        g.add((maintainer, self.FOAF.mbox, Literal('mailto:' + maintainer_email)))
        creator_email = 'demo.creator@org.de'
        creator = BNode()
        g.add((creator, RDF.type, self.FOAF.Organization))
        g.add((creator, self.FOAF.mbox, Literal('mailto:' + creator_email)))

        dataset_ref = URIRef('http://example.org/datasets/1')
        g.add((dataset_ref, RDF.type, self.DCAT.Dataset))
        g.add((dataset_ref, self.DCATDE.maintainer, maintainer))
        g.add((dataset_ref, self.DCT.creator, creator))
        p = self._default_parser_dcatde()

        p.g = g

        datasets = [d for d in p.datasets()]

        self.assertEqual(maintainer_email, datasets[0]['maintainer_email'])
        self.assertEqual(creator_email, datasets[0]['author_email'])

    @helpers.change_config(DCAT_CLEAN_TAGS, 'true')
    def test_tags_clean_tags_on(self):
        g = Graph()

        dataset = URIRef('http://example.org/datasets/1')
        g.add((dataset, RDF.type, self.DCAT.Dataset))
        g.add((dataset, self.DCAT.keyword, Literal(self.INVALID_TAG)))
        p = self._default_parser_dcatde()

        p.g = g

        datasets = [d for d in p.datasets()]

        self.assertIn(self.VALID_TAG, datasets[0]['tags'])
        self.assertNotIn(self.INVALID_TAG, datasets[0]['tags'])

    @helpers.change_config(DCAT_CLEAN_TAGS, 'false')
    def test_tags_clean_tags_off(self):
        g = Graph()

        dataset = URIRef('http://example.org/datasets/1')
        g.add((dataset, RDF.type, self.DCAT.Dataset))
        g.add((dataset, self.DCAT.keyword, Literal(self.INVALID_TAG)))
        p = self._default_parser_dcatde()

        p.g = g

        # when config flag is set to false, bad tags can happen

        datasets = [d for d in p.datasets()]
        self.assertNotIn(self.VALID_TAG, datasets[0]['tags'])
        self.assertIn({'name': self.INVALID_TAG}, datasets[0]['tags'])
