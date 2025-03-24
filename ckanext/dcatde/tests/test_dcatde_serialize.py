#!/usr/bin/env python
# -*- coding: utf8 -*-

import json

import rdflib

from rdflib import Graph, URIRef, Literal, BNode
from ckanext.dcat.profiles import (DCAT, DCT, ADMS, LOCN, SKOS, GSP, RDFS,
                                    VCARD, FOAF, VCARD, SCHEMA, SPDX, DCATAP, XSD)
from ckanext.dcatde.extras import Extras
from ckanext.dcatde.profiles import DCATdeProfile
from ckanext.dcatde.tests.utils import BaseSerializeTest, DCATDE, _get_value_from_extras


class TestDCATdeSerialize(BaseSerializeTest):

    def test_graph_from_dataset(self):
        """ test dcat and dcatde profiles """

        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()
        dataset_ref = URIRef("http://testuri/")

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        # Assert structure of graph - basic values
        extras = dataset_dict["extras"]

        for key in dataset_dict:
            self._check_simple_items(dataset_dict, dataset_ref, key)

        for key in extras:
            self._check_simple_items(dataset_dict, dataset_ref, key)

        # issued, modified
        self.assertEqual(len(list(self.graph.objects(dataset_ref, DCT.issued))), 1,
                         "dct:issued not found")
        self.assertEqual(len(list(self.graph.objects(dataset_ref, DCT.modified))), 1,
                         "dct:modified not found")

        # groups, tags
        self._assert_list(dataset_ref, DCAT.theme,
                         [self.DCAT_THEME_PREFIX + x["name"] for x in dataset_dict["groups"]])
        self._assert_list(dataset_ref, DCAT.keyword,
                         [x["name"] for x in dataset_dict["tags"]])

        # author, maintainer, originator, contributor, publisher
        self._assert_contact_info(dataset_ref, DCATDE.originator)
        self._assert_contact_info(dataset_ref, DCATDE.maintainer)
        self._assert_contact_info(dataset_ref, DCT.contributor)
        self._assert_contact_info(dataset_ref, DCT.creator)
        self._assert_contact_info(dataset_ref, DCT.publisher)

        # contactPoint
        self._assert_contact_point(dataset_ref)

        # temporal
        temporal = list(self.graph.objects(dataset_ref, DCT.temporal))[0]
        self.assertEqual(len(list(self.graph.objects(temporal, DCAT.startDate))), 1,
                         DCAT.startDate + " not found")
        self.assertEqual(len(list(self.graph.objects(temporal, DCAT.endDate))), 1,
                         DCAT.endDate + " not found")

        # spatial
        for spatial in list(self.graph.objects(dataset_ref, DCT.spatial)):
            geonodes = len(list(self.graph.objects(spatial, LOCN.geometry)))
            adminnodes = len(list(self.graph.objects(spatial, LOCN.adminUnitL2)))
            if geonodes > 0:
                self.assertEqual(geonodes, 1, LOCN.geometry + " not present, 1x")
            elif adminnodes > 0:
                self.assertEqual(adminnodes, 1, LOCN.adminUnitL2 + " not present")
            else:
                self.fail("No valid spatial blocks found.")

        # availability
        expected_availability = URIRef('http://publications.europa.eu/resource/authority/planned-availability/AVAILABLE')
        self._assert_simple_value(dataset_ref, DCATAP.availability, expected_availability)

        # lists in extras
        self._assert_list(dataset_ref, DCT.language,
                         _get_value_from_extras(extras, "language"))
        self._assert_list(dataset_ref, DCT.conformsTo,
                         _get_value_from_extras(extras, "conforms_to"))
        self._assert_list(dataset_ref, ADMS.identifier,
                         _get_value_from_extras(extras, "alternate_identifier"))
        self._assert_list(dataset_ref, DCT.relation,
                         _get_value_from_extras(extras, "used_datasets"))
        self._assert_list(dataset_ref, DCT.hasVersion,
                         _get_value_from_extras(extras, "has_version"))
        self._assert_list(dataset_ref, DCT.isVersionOf,
                         _get_value_from_extras(extras, "is_version_of"))
        self._assert_list(dataset_ref, DCATDE.politicalGeocodingURI,
                         _get_value_from_extras(extras, "politicalGeocodingURI"))
        self._assert_list(dataset_ref, DCATDE.geocodingDescription,
                         _get_value_from_extras(extras, "geocodingText"))
        self._assert_list(dataset_ref, DCATDE.legalBasis,
                         _get_value_from_extras(extras, "legalbasisText"))
        self._assert_list(dataset_ref, DCATDE.contributorID,
                         _get_value_from_extras(extras, "contributorID"))
        self._assert_list(dataset_ref, DCT.references,
                         _get_value_from_extras(extras, "references"))

        # resources
        resource = dataset_dict["resources"][0]
        resource_ref = list(self.graph.objects(dataset_ref, DCAT.distribution))[0]
        resource_extras = resource["extras"]

        for key in resource:
            self._check_simple_items(resource, resource_ref, key)

        for key in resource_extras:
            self._check_simple_items(resource, resource_ref, key)

        # size
        self.assertEqual(len(list(self.graph.objects(resource_ref, DCAT.byteSize))), 1,
                         DCAT.byteSize + " not found")

        # hash
        self.assertEqual(len(list(self.graph.objects(resource_ref, SPDX.checksum))), 1,
                         SPDX.checksum + " not found")

        # lists
        self._assert_list(resource_ref, DCT.language,
                         _get_value_from_extras(resource_extras, "language"))
        self._assert_list(resource_ref, DCT.conformsTo,
                         _get_value_from_extras(resource_extras, "conforms_to"))

        # access services
        object_list = [x for x in self.graph.objects(resource_ref, DCAT.accessService)]
        self.assertEqual(len(object_list), 2, "dcat:accessService not found.")
        access_services = json.loads(resource["access_services"])

        for object in object_list:
            title_objects = [x for x in self.graph.objects(object, DCT.title)]
            self.assertEqual(len(title_objects), 1)

            access_service = self._get_dict_from_list(access_services, 'title',
                                                                str(title_objects[0]))
            self.assertTrue(access_service)

            self._assert_simple_value(object, DCATDE.licenseAttributionByText,
                                  Literal(access_service.get('licenseAttributionByText')))

    def test_access_service_fields_invalid_json(self):

        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()
        dataset_ref = URIRef("http://testuri/")

        access_service_list = "Invalid Json list"

        dataset_dict['resources'][0]['access_services'] = access_service_list

        ### execute ###
        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        resource_ref = list(self.graph.objects(dataset_ref, DCAT.distribution))[0]

        ### assert ###
        object_list = [x for x in self.graph.objects(resource_ref, DCAT.accessService)]
        self.assertEqual(len(object_list), 0, "Invalid Json but dcat:accessService was found.")

    def test_graph_from_dataset_contact_point_values(self):

        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()

        # remove fields processed by ckanext-dcat default profile as default values for DCAT.ContactPoint
        dataset_dict.pop('maintainer')
        dataset_dict.pop('maintainer_email')
        dataset_dict.pop('author')
        dataset_dict.pop('author_email')

        dataset_ref = URIRef("http://testuri/")

        ### execute ###
        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        ### assert ###
        # contactPoint
        self._assert_contact_point(dataset_ref)

    def test_graph_from_dataset_only_dcatde_contact_point_values(self):

        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()

        dataset_ref = URIRef("http://testuri/")

        ### execute ###
        dcatde = DCATdeProfile(self.graph, False)
        dcatde._graph_from_dataset_dcatapde(dataset_dict, dataset_ref)

        ### assert ###
        # contactPoint
        self._assert_contact_point(dataset_ref, [VCARD.fn, VCARD.hasEmail])

    def test_graph_from_dataset_contact_point_maintainer_tel_fallback(self):
        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()

        # remove every field from extras which starts with 'contact_'
        dataset_dict['extras'] = [item for item in dataset_dict['extras'] if not item['key'].startswith('contact')]

        dataset_ref = URIRef("http://testuri/")

        ### execute ###
        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        ### assert ###
        # contactPoint name and email are set by dcat
        # everything else should be set by dcatde using maintainer_xxx as fallback
        self._assert_contact_point(dataset_ref, [VCARD.hasURL])

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

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        # assert
        values = _get_value_from_extras(dataset_dict["extras"], 'contributorID')
        self.assertEqual(len(values), len(values_in_dataset_dict))
        for item in [
            ('contributorID', DCATDE.contributorID, [Literal, URIRef])
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

    def test_resource_last_modified_fallback(self):
        ''' Check if last_modified is used as fallback without other date fields'''
        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()
        dataset_ref = URIRef("http://testuri/")
        last_modified_value = '2016-06-26T15:21:09'
        resource = dataset_dict['resources'][0]
        extras = Extras(resource['extras'])

        resource.pop('modified', None)
        resource.pop('metadata_modified', None)
        [extras.remove(key) for key in ['modified', 'metadata_modified'] if extras.key(key)]
        resource['last_modified'] = last_modified_value

        # execute
        self.graph = rdflib.Graph()

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        resource_ref = list(self.graph.objects(dataset_ref, DCAT.distribution))[0]

        # assert
        assert len([x for x in self.graph.objects(resource_ref, DCT.modified)]) == 1

        self._assert_simple_value(resource_ref, DCT.modified,
                                  Literal(last_modified_value, datatype=XSD.dateTime))

    def test_resource_last_modified_fallback_when_metadata_modified_exists(self):
        ''' Check if last_modified is used as fallback and has a higher priority than metadata_modified'''
        ### prepare ###
        self.graph = rdflib.Graph()
        dataset_dict = self._get_default_dataset_dict()
        dataset_ref = URIRef("http://testuri/")
        last_modified_value = '2016-06-26T15:21:09'
        metadata_modified_value = '2015-06-26T15:21:09'
        resource = dataset_dict['resources'][0]
        extras = Extras(resource['extras'])

        resource.pop('modified', None)
        [extras.remove(key) for key in ['modified', 'metadata_modified'] if extras.key(key)]
        resource['metadata_modified'] = metadata_modified_value
        resource['last_modified'] = last_modified_value

        # execute
        self.graph = rdflib.Graph()

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)

        resource_ref = list(self.graph.objects(dataset_ref, DCAT.distribution))[0]

        # assert
        assert len([x for x in self.graph.objects(resource_ref, DCT.modified)]) == 1

        self._assert_simple_value(resource_ref, DCT.modified,
                                  Literal(last_modified_value, datatype=XSD.dateTime))
