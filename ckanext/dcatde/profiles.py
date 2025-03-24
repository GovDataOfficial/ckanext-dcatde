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
from ckanext.dcat.profiles import EuropeanDCATAP2Profile, CleanedURIRef
from ckanext.dcat.profiles.base import URIRefOrLiteral
from ckanext.dcat.utils import resource_uri, DCAT_CLEAN_TAGS
import ckanext.dcatde.dataset_utils as ds_utils

# copied from ckanext.dcat.profiles
DCT = Namespace("http://purl.org/dc/terms/")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
DCATAP = Namespace("http://data.europa.eu/r5r/")
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

DCAT_THEME_PREFIX = "http://publications.europa.eu/resource/authority/data-theme/"

namespaces = {
    # copied from ckanext.dcat.profiles
    'dct': DCT,
    'dcat': DCAT,
    'dcatap': DCATAP,
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

class DCATdeProfile(EuropeanDCATAP2Profile):
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

        for contact in self.g.objects(dataset_ref, predicate):

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

            if self._is_contact_in_dict(dataset_dict, contact):
                break

    def _is_contact_in_dict(self, dataset_dict, contact_obj):

        contact_details = self._get_contact_details(contact_obj)

        for key in ("uri", "identifier", "name", "email"):
            contact_field =  ds_utils.get_extras_field(dataset_dict, 'contact_{0}'.format(key))
            if contact_field and contact_details[key] == contact_field['value']:
                return True

        return False

    def _get_contact_details(self, contact_obj):

        contact = {}

        contact['uri'] = (str(contact_obj) if isinstance(contact_obj,
                                                         URIRef) else '')

        contact['name'] = self._get_vcard_property_value(contact_obj, VCARD.hasFN, VCARD.fn)

        contact['email'] = self._without_mailto(self._get_vcard_property_value(contact_obj, VCARD.hasEmail))

        return contact

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
            return str(value).replace(PREFIX_TEL, u'')
        else:
            return value

    def _iterate_graph_and_dict_list(self, subject, predicate, dict_list, object_dict_ref):
        '''
        Iterates over all subject objects in graph and dict list and returns all matching objects.
        '''
        for object in self.g.objects(subject, predicate):
            object_ref = str(object)
            for object_dict in dict_list:
                # Match object in graph and in dict
                if object_dict and (object_ref == object_dict.get('uri')) or \
                        (object_ref == object_dict.get(object_dict_ref)):
                    yield object, object_dict

    def parse_dataset(self, dataset_dict, dataset_ref):
        """ Transforms DCAT-AP.de-Data to CKAN-Dictionary """

        # call super method
        super(DCATdeProfile, self).parse_dataset(dataset_dict, dataset_ref)

        # DCAT-AP.de properties
        self._parse_dataset_dcatapde(dataset_dict, dataset_ref)

    def _parse_dataset_dcatapde(self, dataset_dict, dataset_ref):
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

        resource_list = dataset_dict.get('resources', [])

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
            legalbasis_text = dcatde_namespace.legalBasis
            geocoding_text = dcatde_namespace.geocodingDescription

            if (dcatde_namespace == DCATDE_1_0):
                legalbasis_text = DCATDE_1_0.legalbasisText
                geocoding_text = DCATDE_1_0.geocodingText

            # List fields
            for key, predicate, in (
                   ('contributorID', dcatde_namespace.contributorID),
                   ('politicalGeocodingURI', dcatde_namespace.politicalGeocodingURI),
                   ('legalbasisText', legalbasis_text),
                   ('geocodingText', geocoding_text),
                   ):
                values = self._object_value_list(dataset_ref, predicate)
                if values:
                    ds_utils.set_extras_field(dataset_dict, key, json.dumps(values))

            self._parse_contact(dataset_dict, dataset_ref, dcatde_namespace.originator, 'originator', True)
            self._parse_contact(dataset_dict, dataset_ref, dcatde_namespace.maintainer, 'maintainer', False)

            # Add additional distribution fields
            for distribution, resource_dict in self. _iterate_graph_and_dict_list(
                dataset_ref, DCAT.distribution, resource_list, 'distribution_ref'):
                for key, predicate in (
                        ('licenseAttributionByText', dcatde_namespace.licenseAttributionByText),
                        ('plannedAvailability', dcatde_namespace.plannedAvailability)
                ):
                    value = self._object_value(distribution, predicate)
                    if value:
                        resource_dict[key] = value
        # -- end loop over dcatde namespaces --

        # additions in other namespaces than DCATDE
        self._parse_contact(dataset_dict, dataset_ref, DCT.contributor, 'contributor', True)
        self._parse_contact(dataset_dict, dataset_ref, DCT.creator, 'author', False)

        # Simple additional fields to DCAT-AP
        for key, predicate in (
                ('metadata_original_html', DCAT.landingPage),
                ('granularity', DCAT.granularity),
                ('availability', DCATAP.availability)
                ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                ds_utils.set_extras_field(dataset_dict, key, value)
        # List fields
        for key, predicate, in (
               ('references', DCT.references),
               ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                ds_utils.set_extras_field(dataset_dict, key, json.dumps(values))

        # Add additional distribution fields
        for distribution, resource_dict in self._iterate_graph_and_dict_list(
            dataset_ref, DCAT.distribution, resource_list, 'distribution_ref'):
            # Access services
            access_service_list = json.loads(self._get_dict_value(resource_dict, 'access_services', '[]'))
            for access_service, access_service_dict in self._iterate_graph_and_dict_list(
                distribution, DCAT.accessService, access_service_list, 'access_service_ref'):
                #  Simple values
                for key, predicate in (
                        ('licenseAttributionByText', DCATDE.licenseAttributionByText), # current version
                        ):
                    value = self._object_value(access_service, predicate)
                    if value:
                        access_service_dict[key] = value

            if access_service_list:
                resource_dict['access_services'] = json.dumps(access_service_list)

        # dcat:contactPoint: Saves more fields in addition to the default profile in ckanext-dcat
        self._parse_contact_vcard(dataset_dict, dataset_ref, DCAT.contactPoint, 'contact')

        # Groups
        groups = self._get_dataset_value(dataset_dict, 'groups')

        if not groups:
            groups = []

        for obj in self.g.objects(dataset_ref, DCAT.theme):
            current_theme = str(obj)

            if current_theme.startswith(DCAT_THEME_PREFIX):
                group = current_theme.replace(DCAT_THEME_PREFIX, '').lower()
                groups.append({'id': group, 'name': group})

        dataset_dict['groups'] = groups

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        """ Transforms CKAN-Dictionary to DCAT-AP.de-Data """

        # call super method
        super(DCATdeProfile, self).graph_from_dataset(
            dataset_dict, dataset_ref
        )

        # DCAT-AP.de properties
        self._graph_from_dataset_dcatapde(dataset_dict, dataset_ref)

    def _graph_from_dataset_dcatapde(self, dataset_dict, dataset_ref):
        """ Transforms CKAN-Dictionary to DCAT-AP.de-Data """

        g = self.g

        # bind namespaces to have readable names in RDF Document
        for prefix, namespace in namespaces.items():
            g.bind(prefix, namespace)

        # Simple additional fields
        items = [
            ('qualityProcessURI', DCATDE.qualityProcessURI, None, URIRef),
            ('metadata_original_html', DCAT.landingPage, None, URIRef),
            ('politicalGeocodingLevelURI', DCATDE.politicalGeocodingLevelURI, None, URIRef),
            ('granularity', DCAT.granularity, None, URIRefOrLiteral),
            ('availability', DCATAP.availability, None, URIRefOrLiteral)
        ]
        self._add_triples_from_dict(dataset_dict, dataset_ref, items)

        # Additional Lists
        items = [
            ('contributorID', DCATDE.contributorID, None, URIRefOrLiteral),
            ('politicalGeocodingURI', DCATDE.politicalGeocodingURI, None, URIRef),
            ('legalbasisText', DCATDE.legalBasis, None, Literal),
            ('geocodingText', DCATDE.geocodingDescription, None, Literal),
            ('references', DCT.references, None, URIRefOrLiteral)
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

        # Adds additional fields to contact_point. For compatibility reasons, some "maintainer_" fields used
        # as fallback.
        items = [
            ('contact_url', VCARD.hasURL, None, URIRef, None),
            ('contact_tel', VCARD.hasTelephone, 'maintainer_tel', URIRef, self._add_tel)
        ]
        for field_name, predicate, fallback, type, v_modifier in items:
            value = self._get_dataset_value(dataset_dict, field_name)
            if not value and fallback:
                field_name = fallback
                value = self._get_dataset_value(dataset_dict, field_name)
            if value:
                contact_point = self._get_or_create_contact_point(dataset_dict, dataset_ref)
                self._add_triple_from_dict(dataset_dict, contact_point, predicate, field_name, _type=type,
                                           value_modifier=v_modifier)

        # Add contact postal data to contact_point. If not available, use maintainer postal data
        # to ensure backwards compatibility
        vcard_mapping = {
            'street': VCARD.hasStreetAddress,
            'city': VCARD.hasLocality,
            'zip': VCARD.hasPostalCode,
            'country': VCARD.hasCountryName
        }
        for vc_name, vcard_predicate in vcard_mapping.items():
            vcard_fld = self._get_dataset_value(dataset_dict, 'contact_' + vc_name) or self._get_dataset_value(
                dataset_dict, 'maintainer_' + vc_name)
            if vcard_fld:
                contact_point = self._get_or_create_contact_point(dataset_dict, dataset_ref)
                g.add((contact_point, vcard_predicate, Literal(vcard_fld)))

        # Groups
        groups = self._get_dataset_value(dataset_dict, 'groups')
        for group in groups:
            group_name = group['name']
            if group_name:
                g.add((dataset_ref, DCAT.theme, CleanedURIRef(DCAT_THEME_PREFIX + group_name.upper())))

        # used_datasets
        items = [
            ('used_datasets', DCT.relation, None, URIRef),
        ]
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, items)

        # Enhance Distributions
        for resource_dict in dataset_dict.get('resources', []):
            distribution = CleanedURIRef(resource_uri(resource_dict))

            items = [
                ('licenseAttributionByText', DCATDE.licenseAttributionByText, None, Literal),
                ('plannedAvailability', DCATDE.plannedAvailability, None, URIRef)
            ]
            self._add_triples_from_dict(resource_dict, distribution, items)

            # Override modified date, if necessary
            if self._get_dict_value(resource_dict, 'last_modified') and \
                    not self._get_dict_value(resource_dict, 'modified'):
                # remove existing values to prevent duplicates
                g.remove((distribution, DCT.modified, None))
                # add new value
                items = [
                    ('last_modified', DCT.modified, None, Literal),
                ]
                self._add_date_triples_from_dict(resource_dict, distribution, items)

            try:
                access_service_list = json.loads(self._get_dict_value(resource_dict, 'access_services', '[]'))
                # Access service
                for access_service_dict in access_service_list:

                    access_service_uri = access_service_dict.get('uri')
                    if access_service_uri:
                        access_service = CleanedURIRef(access_service_uri)
                    else:
                        access_service = BNode(access_service_dict.get('access_service_ref'))

                     #  Simple values
                    items = [
                        ('licenseAttributionByText', DCATDE.licenseAttributionByText, None, Literal),
                    ]
                    self._add_triples_from_dict(access_service_dict, access_service, items)

                resource_dict['access_services'] = json.dumps(access_service_list)
            except ValueError:
                pass

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        """ Creates a Catalog representation, will not be used for now """

        # call super method
        super(DCATdeProfile, self).graph_from_catalog(
            catalog_dict, catalog_ref
        )


def _munge_tag(tag):
    '''Cleans a given tag from special characters.'''
    tag = tag.lower().strip()
    tag = re.sub(u'[^a-zA-ZÄÖÜäöüß0-9 \\-_\\.]', '', tag).replace(' ', '-')
    return _munge_to_length(tag, model.MIN_TAG_LENGTH, model.MAX_TAG_LENGTH)
