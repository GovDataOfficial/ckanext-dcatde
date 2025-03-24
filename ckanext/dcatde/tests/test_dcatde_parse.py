#!/usr/bin/env python
# -*- coding: utf8 -*-

import json

from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import RDF
from ckantoolkit.tests import helpers
from ckanext.dcat.utils import DCAT_CLEAN_TAGS
from ckanext.dcatde.profiles import DCATdeProfile
from ckanext.dcat.profiles import (DCAT, DCT, ADMS, LOCN, SKOS, GSP, RDFS,
                                    VCARD, FOAF, VCARD)
from ckanext.dcatde.tests.utils import BaseParseTest, DCATDE, _get_value_from_extras

class TestDCATdeParse(BaseParseTest):

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

            # dcatap:availability
            self._assert_extras_string(extras, 'availability',
                                      'http://publications.europa.eu/resource/authority/planned-availability/AVAILABLE')

            # dct:references
            self._assert_extras_list_serialized(extras, 'references',
                                           ['https://musterdatenkatalog.de/def/musterdatensatz/abfallwirtschaft/abfallkalender'])

            # access services
            access_service_list = json.loads(dist1.get('access_services'))
            self.assertEqual(len(access_service_list), 1)
            self.assertEqual(access_service_list[0].get('licenseAttributionByText'), 'License text')

        else:
            self.assertEqual(len([x for x in extras if x["key"] == 'granularity']), 0)
            self.assertEqual(len([x for x in extras if x["key"] == 'availability']), 0)

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
        self.assertEqual(dist1.get('plannedAvailability'),
                         'http://dcat-ap.de/def/plannedAvailability/experimental')
        self.assertEqual(dist1.get('licenseAttributionByText'),
                         u'Freie und Hansestadt Hamburg, Behörde für Umwelt und Energie, 2016')
        self.assertEqual(dist1.get('license'),
                         "http://dcat-ap.de/def/licenses/dl-by-de/2_0")
        self.assertEqual(dist1.get('size'), 685246)

        self.assertEqual(dist2.get('plannedAvailability'),
                         'http://dcat-ap.de/def/plannedAvailability/available')
        self.assertEqual(dist2.get('licenseAttributionByText'),
                         u'Freie und Hansestadt Hamburg, Behörde für Umwelt und Energie, 2015')
        self.assertEqual(dist2.get('license'),
                         "http://dcat-ap.de/def/licenses/dl-by-de/2_0")
        self.assertEqual(dist2.get('size'), 222441)


        # some non-dcatde fields
        self._assert_extras_list_serialized(extras, 'alternate_identifier',
                                           ['4635D337-4805-4C32-A211-13F8C038BF27'])

        # dcat:contactPoint
        self._assert_extras_string(extras, 'contact_email', u'michael.schroeder@bue.hamburg.de')
        self._assert_extras_string(extras, 'contact_url', u'http://michaelschroeder.de')
        self._assert_extras_string(extras, 'contact_name', u'Herr Dr. Michael Schröder')
        self._assert_extras_string(extras, 'contact_tel', u'+49 40 4 28 40 - 3494')
        self._assert_extras_string(extras, 'contact_street', u'Beispielstraße 4')
        self._assert_extras_string(extras, 'contact_city', u'Beispielort')
        self._assert_extras_string(extras, 'contact_zip', u'12345')
        self._assert_extras_string(extras, 'contact_country', u'DE')

        # Groups
        self.assertEqual(len(dataset['groups']), 2)
        self.assertTrue({'id': 'envi', 'name': 'envi'} in dataset['groups'])
        self.assertTrue({'id': 'agri', 'name': 'agri'} in dataset['groups'])

        # Keywords
        self._assert_tag_list(
            dataset,
            [u'Karte', u'hmbtg_09_geodaten', u'Grundwasser', u'Bodenschutz', u'Geodaten',
             u'Umwelt und Klima', u'hmbtg', u'opendata', u'Thematische Karte'])

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
        self.assertIn(u'Behörde für Umwelt und Energie (BUE), Amt für Umweltschutz', _get_value_from_extras(extras, 'publisher_name'))
        self.assertIn(u'Herr Dr. Michael Schröder', _get_value_from_extras(extras, 'contact_name'))
        # Resources
        resources = dataset.get('resources')
        self.assertEqual(len(resources), 2)
        for res in resources:
            # Title and description random
            self.assertIn(u'Naturräume Geest und Marsch', res.get('name'))
            self.assertIn(
                u'Das ist eine deutsche Beschreibung der Distribution',
                res.get('description'))

    def test_parse_dataset_distribution_without_uri(self):

        license_attribution_by_text = u'Freie und Hansestadt Hamburg, Behörde für Umwelt und Energie, 2016'
        planned_availability = u"http://dcat-ap.de/def/plannedAvailability/available"
        data = u'''<?xml version="1.0" encoding="utf-8" ?>
        <rdf:RDF
         xmlns:dct="http://purl.org/dc/terms/"
         xmlns:dcat="http://www.w3.org/ns/dcat#"
         xmlns:dcatde="http://dcat-ap.de/def/dcatde/"
         xmlns:schema="http://schema.org/"
         xmlns:time="http://www.w3.org/2006/time"
         xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
        <dcat:Dataset rdf:about="http://example.org">
            <dcat:distribution>
                <dcat:Distribution>
                    <dcat:accessURL rdf:resource="http://geodienste.hamburg.de/darf_nicht_die_gleiche_url_wie_downloadurl_sein_da_es_sonst_nicht_angezeigt_wird"/>
                    <dct:description>Das ist eine deutsche Beschreibung der Distribution</dct:description>
                    <dct:issued rdf:datatype="http://www.w3.org/2001/XMLSchema#date">2017-02-27</dct:issued>
                    <dct:title>Download WFS Naturräume Geest und Marsch (GML)</dct:title>
                    <dct:modified rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2017-03-07T10:00:00</dct:modified>
                    <dcatde:licenseAttributionByText>{by_text}</dcatde:licenseAttributionByText>
                    <dcatde:plannedAvailability rdf:resource="{planned_availability}"/>
                </dcat:Distribution>
            </dcat:distribution>
        </dcat:Dataset>
        </rdf:RDF>
        '''.format(by_text=license_attribution_by_text, planned_availability=planned_availability)

        p = self._default_parser_dcatde()

        p.parse(data)

        datasets = [d for d in p.datasets()]
        self.assertEqual(len(datasets), 1)
        dataset = datasets[0]

        # Resources
        resources = dataset.get('resources')
        self.assertEqual(len(resources), 1)
        resource_dict = resources[0]

        assert resource_dict.get('licenseAttributionByText') == license_attribution_by_text
        assert resource_dict.get('plannedAvailability') == planned_availability

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
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        # author/creator, maintainer, originator, contributor, publisher
        originator = self._create_contact_node(g, 'originator')
        g.add((dataset_ref, DCATDE.originator, originator))
        maintainer = self._create_contact_node(g, 'maintainer')
        g.add((dataset_ref, DCATDE.maintainer, maintainer))
        contributor = self._create_contact_node(g, 'contributor')
        g.add((dataset_ref, DCT.contributor, contributor))
        creator = self._create_contact_node(g, 'creator')
        g.add((dataset_ref, DCT.creator, creator))
        publisher = self._create_contact_node(g, 'publisher')
        g.add((dataset_ref, DCT.publisher, publisher))

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
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, VCARD.Organization))
        g.add((contact_point, VCARD.hasURL, Literal('http://contact-point-url.de')))
        g.add((contact_point, VCARD.hasTelephone, Literal('+490531-24262-10')))
        g.add((dataset_ref, DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'contact_url', u'http://contact-point-url.de')
        self._assert_extras_string(extras, 'contact_tel', u'+490531-24262-10')

    def test_dataset_contact_point_vcard_hasURL_hasTelephone_uriref(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, VCARD.Organization))
        g.add((contact_point, VCARD.hasURL, URIRef('http://contact-point-url.de')))
        g.add((contact_point, VCARD.hasTelephone, URIRef('tel:+490531-24262-10')))
        g.add((dataset_ref, DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'contact_url', u'http://contact-point-url.de')
        self._assert_extras_string(extras, 'contact_tel', u'+490531-24262-10')

    def test_dataset_contact_point_vcard_hasURL_hasTelephone_hasValue_literal(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, VCARD.Organization))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, VCARD.hasURL, Literal('http://contact-point-url.de'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, VCARD.hasTelephone, Literal('+490531-24262-10'))
        g.add((dataset_ref, DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'contact_url', u'http://contact-point-url.de')
        self._assert_extras_string(extras, 'contact_tel', u'+490531-24262-10')

    def test_dataset_contact_point_vcard_hasURL_hasTelephone_hasValue_uriref(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, VCARD.Organization))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, VCARD.hasURL, URIRef('http://contact-point-url.de'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, VCARD.hasTelephone, URIRef('tel:+490531-24262-10'))
        g.add((dataset_ref, DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'contact_url', u'http://contact-point-url.de')
        self._assert_extras_string(extras, 'contact_tel', u'+490531-24262-10')

    def test_dataset_contact_point_vcard_address_has_fields_direct(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, VCARD.Organization))
        g.add((contact_point, VCARD.hasLocality, Literal('Berlin')))
        g.add((contact_point, VCARD.hasStreetAddress, Literal('Hauptstraße 1')))
        g.add((contact_point, VCARD.hasPostalCode, Literal('12345')))
        g.add((contact_point, VCARD.hasCountryName, Literal('Deutschland')))
        g.add((dataset_ref, DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'contact_city', u'Berlin')
        self._assert_extras_string(extras, 'contact_street', u'Hauptstraße 1')
        self._assert_extras_string(extras, 'contact_zip', u'12345')
        self._assert_extras_string(extras, 'contact_country', u'Deutschland')

    def test_dataset_contact_point_vcard_address_has_fields_direct_with_hasvalue(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, VCARD.Organization))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, VCARD.hasLocality, Literal('Berlin'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, VCARD.hasStreetAddress, Literal('Hauptstraße 1'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, VCARD.hasPostalCode, Literal('12345'))
        self._add_vcard_property_with_hasvalue(
            g, contact_point, VCARD.hasCountryName, Literal('Deutschland'))
        g.add((dataset_ref, DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'contact_city', u'Berlin')
        self._assert_extras_string(extras, 'contact_street', u'Hauptstraße 1')
        self._assert_extras_string(extras, 'contact_zip', u'12345')
        self._assert_extras_string(extras, 'contact_country', u'Deutschland')

    def test_dataset_contact_point_vcard_address_has_fields_within_address_object(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, VCARD.Organization))
        address = BNode()
        g.add((address, RDF.type, VCARD.Address))
        g.add((address, VCARD.hasLocality, Literal('Berlin')))
        g.add((address, VCARD.hasStreetAddress, Literal('Hauptstraße 1')))
        g.add((address, VCARD.hasPostalCode, Literal('12345')))
        g.add((address, VCARD.hasCountryName, Literal('Deutschland')))
        g.add((contact_point, VCARD.hasAddress, address))
        g.add((dataset_ref, DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'contact_city', u'Berlin')
        self._assert_extras_string(extras, 'contact_street', u'Hauptstraße 1')
        self._assert_extras_string(extras, 'contact_zip', u'12345')
        self._assert_extras_string(extras, 'contact_country', u'Deutschland')

    def test_dataset_contact_point_vcard_address_has_fields_within_address_object_with_hasvalue(self):
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contact_point = BNode()
        g.add((contact_point, RDF.type, VCARD.Organization))
        address = BNode()
        g.add((address, RDF.type, VCARD.Address))
        self._add_vcard_property_with_hasvalue(
            g, address, VCARD.hasLocality, Literal('Berlin'))
        self._add_vcard_property_with_hasvalue(
            g, address, VCARD.hasStreetAddress, Literal('Hauptstraße 1'))
        self._add_vcard_property_with_hasvalue(
            g, address, VCARD.hasPostalCode, Literal('12345'))
        self._add_vcard_property_with_hasvalue(
            g, address, VCARD.hasCountryName, Literal('Deutschland'))
        g.add((contact_point, VCARD.hasAddress, address))
        g.add((dataset_ref, DCAT.contactPoint, contact_point))

        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]
        extras = dataset.get('extras')
        self._assert_extras_string(extras, 'contact_city', u'Berlin')
        self._assert_extras_string(extras, 'contact_street', u'Hauptstraße 1')
        self._assert_extras_string(extras, 'contact_zip', u'12345')
        self._assert_extras_string(extras, 'contact_country', u'Deutschland')

    def test_dataset_contact_point_vcard_multiple_nodes_matched_name(self):

        # prepare
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contacts = [
            {'name': 'Herr Dr. Michael Schröder', 'url': 'http://michaelschroeder.de'},
            {'name': 'Herr Dr. Max Mustermann', 'url': 'http://maxmustermann.de'}]

        for contact in contacts:
            contact_point = BNode()
            g.add((contact_point, RDF.type, VCARD.Organization))
            self._add_vcard_property_with_hasvalue(
                g, contact_point, VCARD.hasFN, Literal(contact['name']))
            self._add_vcard_property_with_hasvalue(
                g, contact_point, VCARD.hasURL, Literal(contact['url']))
            g.add((dataset_ref, DCAT.contactPoint, contact_point))

        # execute
        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]

        extras = dataset.get('extras')

        # test if contact has been matched using name
        contact_name_value = _get_value_from_extras(extras, 'contact_name')
        contact_url_value = _get_value_from_extras(extras, 'contact_url')
        for contact in contacts:
            if contact_name_value == contact['name']:
                self.assertEqual(contact['url'], contact_url_value)


    def test_dataset_contact_point_vcard_multiple_nodes_matched_email(self):

        # prepare
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contacts = [
            {'email': 'michael.schroeder@bue.hamburg.de', 'url': 'http://michaelschroeder.de'},
            {'email': 'max_mustermann@bue.hamburg.de', 'url': 'http://maxmustermann.de'}]

        for contact in contacts:
            contact_point = BNode()
            g.add((contact_point, RDF.type, VCARD.Organization))
            self._add_vcard_property_with_hasvalue(
                g, contact_point, VCARD.hasEmail, Literal(contact['email']))
            self._add_vcard_property_with_hasvalue(
                g, contact_point, VCARD.hasURL, Literal(contact['url']))
            g.add((dataset_ref, DCAT.contactPoint, contact_point))

        # execute
        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]

        extras = dataset.get('extras')

        # test if contact has been matched using email
        contact_email_value = _get_value_from_extras(extras, 'contact_email')
        contact_url_value = _get_value_from_extras(extras, 'contact_url')
        for contact in contacts:
            if contact_email_value == contact['email']:
                self.assertEqual(contact['url'], contact_url_value)

    def test_dataset_contact_point_vcard_multiple_nodes_matched_uri(self):

        # prepare
        g = Graph()

        dataset_ref = URIRef("http://example.org/datasets/1")
        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        contacts = [
            {'uri': 'http://example.org/contactpoint/1', 'tel': '+490522-22222-22'},
            {'uri': 'http://example.org/contactpoint/2', 'tel': '+490533-33333-33'}]

        for contact in contacts:
            contact_point = URIRef(contact['uri'])
            g.add((contact_point, RDF.type, VCARD.Organization))
            self._add_vcard_property_with_hasvalue(
                g, contact_point, VCARD.hasTelephone, Literal(contact['tel']))
            g.add((dataset_ref, DCAT.contactPoint, contact_point))

        # execute
        p = self._default_parser_dcatde()

        p.g = g

        dataset = [d for d in p.datasets()][0]

        extras = dataset.get('extras')


        # test if contact has been matched using uri
        contact_uri_value = _get_value_from_extras(extras, 'contact_uri')
        contact_tel_value = _get_value_from_extras(extras, 'contact_tel')
        for contact in contacts:
            if contact_uri_value == contact['uri']:
                self.assertEqual(contact['tel'], contact_tel_value)

    def test_parse_dataset_remove_mailto_from_email(self):
        g = Graph()

        maintainer_email = 'demo.maintainer@org.de'
        maintainer = BNode()
        g.add((maintainer, RDF.type, FOAF.Organization))
        g.add((maintainer, FOAF.mbox, Literal('mailto:' + maintainer_email)))
        creator_email = 'demo.creator@org.de'
        creator = BNode()
        g.add((creator, RDF.type, FOAF.Organization))
        g.add((creator, FOAF.mbox, Literal('mailto:' + creator_email)))

        dataset_ref = URIRef('http://example.org/datasets/1')
        g.add((dataset_ref, RDF.type, DCAT.Dataset))
        g.add((dataset_ref, DCATDE.maintainer, maintainer))
        g.add((dataset_ref, DCT.creator, creator))
        p = self._default_parser_dcatde()

        p.g = g

        datasets = [d for d in p.datasets()]

        self.assertEqual(maintainer_email, datasets[0]['maintainer_email'])
        self.assertEqual(creator_email, datasets[0]['author_email'])

    @helpers.change_config(DCAT_CLEAN_TAGS, 'true')
    def test_tags_clean_tags_on(self):
        g = Graph()

        dataset = URIRef('http://example.org/datasets/1')
        g.add((dataset, RDF.type, DCAT.Dataset))
        g.add((dataset, DCAT.keyword, Literal(self.INVALID_TAG)))
        p = self._default_parser_dcatde()

        p.g = g

        datasets = [d for d in p.datasets()]

        self.assertIn(self.VALID_TAG, datasets[0]['tags'])
        self.assertNotIn(self.INVALID_TAG, datasets[0]['tags'])

    @helpers.change_config(DCAT_CLEAN_TAGS, 'false')
    def test_tags_clean_tags_off(self):
        g = Graph()

        dataset = URIRef('http://example.org/datasets/1')
        g.add((dataset, RDF.type, DCAT.Dataset))
        g.add((dataset, DCAT.keyword, Literal(self.INVALID_TAG)))
        p = self._default_parser_dcatde()

        p.g = g

        # when config flag is set to false, bad tags can happen

        datasets = [d for d in p.datasets()]
        self.assertNotIn(self.VALID_TAG, datasets[0]['tags'])
        self.assertIn({'name': self.INVALID_TAG}, datasets[0]['tags'])

    @helpers.change_config(DCAT_CLEAN_TAGS, 'true')
    def test_tags_clean_tags_min_len(self):
        g = Graph()

        dataset = URIRef('http://example.org/datasets/1')
        g.add((dataset, RDF.type, DCAT.Dataset))
        # tag would become too short without invalid characters, ensure it will still have minimum length
        g.add((dataset, DCAT.keyword, Literal(self.INVALID_TAG_SHORT)))
        p = self._default_parser_dcatde()

        p.g = g

        datasets = [d for d in p.datasets()]
        self.assertNotIn({'name': self.INVALID_TAG_SHORT}, datasets[0]['tags'])
        # depends on ckan.model.MIN_TAG_LENGTH and behaviour of ckan's _munge_to_length
        self.assertIn({'name': u'a_'}, datasets[0]['tags'])

    def test_license_attribution_by_text_access_service(self):

        license_attribution_by_text = 'License text'

        data = u'''<?xml version="1.0" encoding="utf-8" ?>
        <rdf:RDF
         xmlns:dct="http://purl.org/dc/terms/"
         xmlns:dcat="http://www.w3.org/ns/dcat#"
         xmlns:dcatap="http://data.europa.eu/r5r/"
         xmlns:dcatde="http://dcat-ap.de/def/dcatde/"
         xmlns:schema="http://schema.org/"
         xmlns:time="http://www.w3.org/2006/time"
         xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
        <dcat:Dataset rdf:about="http://example.org">
            <dcat:distribution>
                <dcat:Distribution rdf:about="https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98/1">
                    <dct:description>Das ist eine deutsche Beschreibung der Distribution</dct:description>
                    <dct:title>Download WFS Naturräume Geest und Marsch (GML)</dct:title>
                    <dcat:accessService>
                        <dcat:DataService>
                            <dct:title>Sparql-end Point</dct:title>
                            <dcatde:licenseAttributionByText>{as_licenseAttributionByText}</dcatde:licenseAttributionByText>
                        </dcat:DataService>
                    </dcat:accessService>
                </dcat:Distribution>
            </dcat:distribution>
        </dcat:Dataset>
        </rdf:RDF>
        '''.format(as_licenseAttributionByText=license_attribution_by_text)

        p = self._default_parser_dcatde()

        p.parse(data)

        datasets = [d for d in p.datasets()]
        self.assertEqual(len(datasets), 1)
        dataset = datasets[0]

        # Resources
        resources = dataset.get('resources')
        self.assertEqual(len(resources), 1)
        resource_dict = resources[0]

        access_services = resource_dict.get('access_services')
        access_services_list = json.loads(access_services)
        self.assertEqual(len(access_services_list), 1)
        access_service_dict = access_services_list[0]
        assert access_service_dict.get('licenseAttributionByText') == license_attribution_by_text

    def test_license_attribution_by_text_multiple_access_services(self):

        license_attribution_by_text_1 = 'License text 1'
        license_attribution_by_text_2= 'License text 2'
        license_attribution_by_text_3 = 'License text 3'

        data = u'''<?xml version="1.0" encoding="utf-8" ?>
        <rdf:RDF
         xmlns:dct="http://purl.org/dc/terms/"
         xmlns:dcat="http://www.w3.org/ns/dcat#"
         xmlns:dcatap="http://data.europa.eu/r5r/"
         xmlns:dcatde="http://dcat-ap.de/def/dcatde/"
         xmlns:schema="http://schema.org/"
         xmlns:time="http://www.w3.org/2006/time"
         xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
        <dcat:Dataset rdf:about="http://example.org">
            <dcat:distribution>
                <dcat:Distribution rdf:about="https://data.some.org/catalog/datasets/9df8df51-63db-37a8-e044-0003ba9b0d98/1">
                    <dct:description>Das ist eine deutsche Beschreibung der Distribution</dct:description>
                    <dct:title>Download WFS Naturräume Geest und Marsch (GML)</dct:title>
                    <dcat:accessService>
                        <dcat:DataService>
                            <dct:title>{as_licenseAttributionByText_1}</dct:title>
                            <dcatde:licenseAttributionByText>{as_licenseAttributionByText_1}</dcatde:licenseAttributionByText>
                        </dcat:DataService>
                    </dcat:accessService>
                    <dcat:accessService>
                        <dcat:DataService>
                            <dct:title>{as_licenseAttributionByText_2}</dct:title>
                            <dcatde:licenseAttributionByText>{as_licenseAttributionByText_2}</dcatde:licenseAttributionByText>
                        </dcat:DataService>
                    </dcat:accessService>
                    <dcat:accessService>
                        <dcat:DataService>
                            <dct:title>{as_licenseAttributionByText_3}</dct:title>
                            <dcatde:licenseAttributionByText>{as_licenseAttributionByText_3}</dcatde:licenseAttributionByText>
                        </dcat:DataService>
                    </dcat:accessService>
                </dcat:Distribution>
            </dcat:distribution>
        </dcat:Dataset>
        </rdf:RDF>
        '''.format(as_licenseAttributionByText_1=license_attribution_by_text_1,
                   as_licenseAttributionByText_2=license_attribution_by_text_2,
                   as_licenseAttributionByText_3=license_attribution_by_text_3)

        p = self._default_parser_dcatde()

        p.parse(data)

        datasets = [d for d in p.datasets()]
        self.assertEqual(len(datasets), 1)
        dataset = datasets[0]

        # Resources
        resources = dataset.get('resources')
        self.assertEqual(len(resources), 1)
        resource_dict = resources[0]

        access_services = resource_dict.get('access_services')
        access_services_list = json.loads(access_services)
        self.assertEqual(len(access_services_list), 3)
        for access_service_dict in access_services_list:
            assert access_service_dict.get('licenseAttributionByText') == access_service_dict.get('title')