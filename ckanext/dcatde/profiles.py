#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import re

from ckan import model
from ckan.lib.munge import _munge_to_length
from ckan.plugins import toolkit
from ckantoolkit import config
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, SKOS
from ckanext.dcat.profiles import RDFProfile, CleanedURIRef, URIRefOrLiteral
from ckanext.dcat.utils import resource_uri, DCAT_CLEAN_TAGS
import ckanext.dcatde.dataset_utils as ds_utils

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
DCATDE_1_0 = Namespace("http://dcat-ap.de/def/dcatde/1_0/")
DCATDE_1_0_1 = Namespace("http://dcat-ap.de/def/dcatde/1.0.1/")
DCATDE = Namespace("http://dcat-ap.de/def/dcatde/")

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
    'skos': SKOS,
    'locn': LOCN,
    'gsp': GSP,
    'owl': OWL,

    # own extension
    'dcatde': DCATDE
}

PREFIX_TEL = u'tel:'

class DCATdeProfile(RDFProfile):
    """ DCAT-AP.de Profile extension """

    def _add_contact(self, dataset_dict, dataset_ref, predicate, prefix):
        """ Adds a Contact with name, email and url to the graph"""
        if any([
                self._get_dataset_value(dataset_dict, prefix),
                self._get_dataset_value(dataset_dict, prefix + '_name')
        ]):
            new_node = BNode()

            # FOAF.Person and FOAF.Organization are possible, Organization as default.
            contacttype = self._get_dataset_value(dataset_dict,
                                                  prefix + '_contacttype', "Organization")
            if contacttype == "Person":
                self.g.add((new_node, RDF.type, FOAF.Person))
            else:
                self.g.add((new_node, RDF.type, FOAF.Organization))

            self.g.add((dataset_ref, predicate, new_node))

            items = [
                (prefix, FOAF.name, [prefix + '_name'], Literal),
                (prefix + '_email', FOAF.mbox, None, Literal),
                (prefix + '_url', FOAF.homepage, None, URIRef),
                (prefix + '_type', DCT.type, None, URIRef)
            ]
            self._add_triples_from_dict(dataset_dict, new_node, items)

    def _parse_contact(self, dataset_dict, dataset_ref, predicate, prefix, extras_only):
        """ Adds a Contact with name, email and url from the graph to the
        dataset dict. If extras_only is True, all items are stored in extras. Otherwise,
        name and email are stored as top-level dict entries in the dataset."""
        node = self._object(dataset_ref, predicate)

        if node:
            contacttype = self._object(node, RDF.type)
            if contacttype in [FOAF.Agent, FOAF.Person, FOAF.Organization]:
                name = self._object_value(node, FOAF.name)
                email = self._object_value(node, FOAF.mbox)
                url = self._object_value(node, FOAF.homepage)
                dct_type = self._object_value(node, DCT.type)
                ctype_string = "Person" if contacttype == FOAF.Person else "Organization"

                # if the contact has items on top-level, the name has no _name suffix
                name_key = prefix + "_name" if extras_only else prefix

                ds_utils.insert(dataset_dict, name_key, name, extras_only)
                ds_utils.insert(dataset_dict, prefix + "_email", self._without_mailto(email), extras_only)
                ds_utils.insert(dataset_dict, prefix + "_url", url, True)
                ds_utils.insert(dataset_dict, prefix + "_type", dct_type, True)
                ds_utils.insert(dataset_dict, prefix + "_contacttype", ctype_string, True)

    def _parse_contact_vcard(self, dataset_dict, dataset_ref, predicate, prefix):
        """ Adds a Contact of type VCARD from the graph to the dataset dict.
        All items are stored in the extras dict of the dataset with the given prefix."""

        contact = self._object(dataset_ref, predicate)
        contact_url = self._get_vcard_property_value(contact, VCARD.hasURL)
        ds_utils.insert(dataset_dict, prefix + '_url', contact_url, True)

        contact_tel = self._get_vcard_property_value(contact, VCARD.hasTelephone)
        ds_utils.insert(dataset_dict, prefix + '_tel', self._without_tel(contact_tel), True)

        # If hasAddress object contains, use it to read address values from there
        obj_with_address_values = contact
        address = self._object(contact, VCARD.hasAddress)
        if address:
            obj_with_address_values = address
        contact_street = self._get_vcard_property_value(
            obj_with_address_values, VCARD.hasStreetAddress, VCARD['street-address'])
        ds_utils.insert(dataset_dict, prefix + '_street', contact_street, True)
        contact_city = self._get_vcard_property_value(
            obj_with_address_values, VCARD.hasLocality, VCARD.locality)
        ds_utils.insert(dataset_dict, prefix + '_city', contact_city, True)
        contact_zip = self._get_vcard_property_value(
            obj_with_address_values, VCARD.hasPostalCode, VCARD['postal-code'])
        ds_utils.insert(dataset_dict, prefix + '_zip', contact_zip, True)
        contact_country = self._get_vcard_property_value(
            obj_with_address_values, VCARD.hasCountryName, VCARD['country-name'])
        ds_utils.insert(dataset_dict, prefix + '_country', contact_country, True)

    def _get_or_create_contact_point(self, dataset_dict, dataset_ref):
        """
        Returns the contact point object in the graph or a newly created object
        if no one is found in the given graph.
        """
        contact_point_objects = self.g.objects(dataset_ref, DCAT.contactPoint)
        contact_object_list = list(contact_point_objects)

        if len(contact_object_list) == 0:
            contact_uri = self._get_dataset_value(dataset_dict, 'contact_uri')
            if contact_uri:
                contact_details = CleanedURIRef(contact_uri)
            else:
                contact_details = BNode()

            self.g.add((contact_details, RDF.type, VCARD.Organization))
            self.g.add((dataset_ref, DCAT.contactPoint, contact_details))
            return contact_details

        return next(iter(contact_object_list))

    def _add_tel(self, value):
        '''
        Ensures that the given value has an URIRef-compatible tel: prefix.
        Can be used as modifier function for `_add_triple_from_dict`.
        '''
        if value:
            return PREFIX_TEL + self._without_tel(value)
        else:
            return value

    def _without_tel(self, value):
        '''
        Ensures that the given value string has no tel: prefix.
        '''
        if value:
            return unicode(value).replace(PREFIX_TEL, u'')
        else:
            return value

    def parse_dataset(self, dataset_dict, dataset_ref):
        """ Transforms DCAT-AP.de-Data to CKAN-Dictionary """

        # Different implementation of clean tags for keywords
        do_clean_tags = toolkit.asbool(config.get(DCAT_CLEAN_TAGS, False))
        if do_clean_tags:
            cleaned_tags = [_munge_tag(tag) for tag in self._keywords(dataset_ref)]
            tags = [{'name': tag} for tag in cleaned_tags]
            dataset_dict['tags'] = tags

        # Manage different versions of DCATDE namespaces first.
        # Ensure that they are ordered from oldest to newest version, such that older values get overwritten
        # in case of multiple definitions
        dcatde_versions = [
            DCATDE_1_0,
            DCATDE_1_0_1,
            DCATDE
        ]

        # iterate over all namespaces to import as much as possible
        for dcatde_namespace in dcatde_versions:
            # Simple additional fields
            for key, predicate in (
                   ('qualityProcessURI', dcatde_namespace.qualityProcessURI),
                   ('politicalGeocodingLevelURI', dcatde_namespace.politicalGeocodingLevelURI),
                   ):
                value = self._object_value(dataset_ref, predicate)
                if value:
                    ds_utils.set_extras_field(dataset_dict, key, value)

            # geocodingText and legalbasisText got renamed after 1.0, so assign the respective names
            legalbasisTextProperty = dcatde_namespace.legalBasis
            geocodingTextProperty = dcatde_namespace.geocodingDescription

            if (dcatde_namespace == DCATDE_1_0):
                legalbasisTextProperty = DCATDE_1_0.legalbasisText
                geocodingTextProperty = DCATDE_1_0.geocodingText

            # List fields
            for key, predicate, in (
                   ('contributorID', dcatde_namespace.contributorID),
                   ('politicalGeocodingURI', dcatde_namespace.politicalGeocodingURI),
                   ('legalbasisText', legalbasisTextProperty),
                   ('geocodingText', geocodingTextProperty),
                   ):
                values = self._object_value_list(dataset_ref, predicate)
                if values:
                    ds_utils.set_extras_field(dataset_dict, key, json.dumps(values))

            self._parse_contact(dataset_dict, dataset_ref, dcatde_namespace.originator, 'originator', True)
            self._parse_contact(dataset_dict, dataset_ref, dcatde_namespace.maintainer, 'maintainer', False)

            # Add additional distribution fields
            for distribution in self.g.objects(dataset_ref, DCAT.distribution):
                for resource_dict in dataset_dict.get('resources', []):
                    # Match distribution in graph and distribution in ckan-dict
                    if unicode(distribution) == resource_dict.get('uri'):
                        for key, predicate in (
                                ('licenseAttributionByText', dcatde_namespace.licenseAttributionByText),
                                ('plannedAvailability', dcatde_namespace.plannedAvailability)
                        ):
                            value = self._object_value(distribution, predicate)
                            if value:
                                ds_utils.insert_resource_extra(resource_dict, key, value)
        # -- end loop over dcatde namespaces --

        # additions in other namespaces than DCATDE
        self._parse_contact(dataset_dict, dataset_ref, DCT.contributor, 'contributor', True)
        self._parse_contact(dataset_dict, dataset_ref, DCT.creator, 'author', False)

        # Simple additional fields to DCAT-AP 1.1
        for key, predicate in (
                ('metadata_original_html', DCAT.landingPage),
                ('granularity', DCAT.granularity)
                ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                ds_utils.set_extras_field(dataset_dict, key, value)

        # dcat:contactPoint
        # TODO: dcat-ap adds the values to extras.contact_... . Maybe better than maintainer?
        self._parse_contact_vcard(dataset_dict, dataset_ref, DCAT.contactPoint, 'maintainer')

        # Groups
        groups = self._get_dataset_value(dataset_dict, 'groups')

        if not groups:
            groups = []

        for obj in self.g.objects(dataset_ref, DCAT.theme):
            current_theme = unicode(obj)

            if current_theme.startswith(dcat_theme_prefix):
                group = current_theme.replace(dcat_theme_prefix, '').lower()
                groups.append({'id': group, 'name': group})

        dataset_dict['groups'] = groups

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        """ Transforms CKAN-Dictionary to DCAT-AP.de-Data """
        g = self.g

        # bind namespaces to have readable names in RDF Document
        for prefix, namespace in namespaces.iteritems():
            g.bind(prefix, namespace)

        # Simple additional fields
        items = [
            ('qualityProcessURI', DCATDE.qualityProcessURI, None, URIRef),
            ('metadata_original_html', DCAT.landingPage, None, URIRef),
            ('politicalGeocodingLevelURI', DCATDE.politicalGeocodingLevelURI, None, URIRef),
            ('granularity', DCAT.granularity, None, URIRefOrLiteral)
        ]
        self._add_triples_from_dict(dataset_dict, dataset_ref, items)

        # Additional Lists
        items = [
            ('contributorID', DCATDE.contributorID, None, URIRefOrLiteral),
            ('politicalGeocodingURI', DCATDE.politicalGeocodingURI, None, URIRef),
            ('legalbasisText', DCATDE.legalBasis, None, Literal),
            ('geocodingText', DCATDE.geocodingDescription, None, Literal)
        ]
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, items)

        # Add adminUnitL2 for every politicalGeocodingURI value. Compatibility.
        if self._get_dataset_value(dataset_dict, 'politicalGeocodingURI'):
            spatial_ref = BNode()
            g.add((spatial_ref, RDF.type, DCT.Location))
            g.add((dataset_ref, DCT.spatial, spatial_ref))

            items = [
                ('politicalGeocodingURI', LOCN.adminUnitL2, None, URIRef)
            ]
            self._add_list_triples_from_dict(dataset_dict, spatial_ref, items)

        # Contacts
        self._add_contact(dataset_dict, dataset_ref, DCATDE.originator, 'originator')
        self._add_contact(dataset_dict, dataset_ref, DCATDE.maintainer, 'maintainer')
        self._add_contact(dataset_dict, dataset_ref, DCT.contributor, 'contributor')
        self._add_contact(dataset_dict, dataset_ref, DCT.creator, 'author')

        # Add maintainer_url to contact_point
        maintainer_url = self._get_dataset_value(dataset_dict, 'maintainer_url')
        if maintainer_url:
            contact_point = self._get_or_create_contact_point(dataset_dict, dataset_ref)
            self._add_triple_from_dict(dataset_dict, contact_point, VCARD.hasURL, 'maintainer_url',
                                       _type=URIRef)

        # add maintainer_tel to contact_point
        maintainer_tel = self._get_dataset_value(dataset_dict, 'maintainer_tel')
        if maintainer_tel:
            contact_point = self._get_or_create_contact_point(dataset_dict, dataset_ref)
            self._add_triple_from_dict(dataset_dict, contact_point, VCARD.hasTelephone, 'maintainer_tel',
                                       _type=URIRef, value_modifier=self._add_tel)

        # add maintainer postal data to contact_point if available
        vcard_mapping = {
            'street': VCARD.hasStreetAddress,
            'city': VCARD.hasLocality,
            'zip': VCARD.hasPostalCode,
            'country': VCARD.hasCountryName
        }
        for vc_name in vcard_mapping:
            vcard_fld = self._get_dataset_value(dataset_dict, 'maintainer_' + vc_name)
            if vcard_fld:
                contact_point = self._get_or_create_contact_point(dataset_dict, dataset_ref)
                g.add((contact_point, vcard_mapping[vc_name], Literal(vcard_fld)))

        # Groups
        groups = self._get_dataset_value(dataset_dict, 'groups')
        for group in groups:
            group_name_in_dict = group['name']
            if group_name_in_dict:
                g.add((dataset_ref, DCAT.theme, CleanedURIRef(dcat_theme_prefix + group_name_in_dict.upper())))

        # used_datasets
        items = [
            ('used_datasets', DCT.relation, None, URIRef),
        ]
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, items)

        # Enhance Distributions
        for resource_dict in dataset_dict.get('resources', []):
            for distribution in g.objects(dataset_ref, DCAT.distribution):
                # Match distribution in graph and distribution in ckan-dict
                if unicode(distribution) == resource_uri(resource_dict):
                    items = [
                        ('licenseAttributionByText', DCATDE.licenseAttributionByText, None, Literal),
                        ('plannedAvailability', DCATDE.plannedAvailability, None, URIRef)
                    ]
                    self._add_triples_from_dict(resource_dict, distribution, items)


    def graph_from_catalog(self, catalog_dict, catalog_ref):
        """ Creates a Catalog representation, will not be used for now """
        pass


def _munge_tag(tag):
    '''Cleans a given tag from special characters.'''
    tag = tag.lower().strip()
    tag = re.sub(ur'[^a-zA-ZÄÖÜäöüß0-9 \-_\.]', '', tag).replace(' ', '-')
    return _munge_to_length(tag, model.MIN_TAG_LENGTH, model.MAX_TAG_LENGTH)
