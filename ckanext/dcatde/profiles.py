import json

from ckanext.dcat.profiles import RDFProfile
from ckanext.dcat.utils import resource_uri
import ckanext.dcatde.dataset_utils as ds_utils
from rdflib import URIRef, BNode, Literal
from rdflib.namespace import Namespace, RDF, SKOS


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
DCATDE = Namespace("http://dcat-ap.de/def/dcatde/1_0/")

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
                (prefix + '_url', FOAF.homepage, None, URIRef)
            ]
            self._add_triples_from_dict(dataset_dict, new_node, items)

    def _parse_contact(self, dataset_dict, dataset_ref, predicate, prefix, extras_only):
        """ Adds a Contact with name, email and url from the graph to the
        dataset dict. If extras_only is True, all items are stored in extras. Otherwise,
        name and email are stored as top-level dict entries in the dataset."""
        node = self._object(dataset_ref, predicate)

        if node:
            contacttype = self._object(node, RDF.type)
            if contacttype in [FOAF.Person, FOAF.Organization]:
                name = self._object_value(node, FOAF.name)
                email = self._object_value(node, FOAF.mbox)
                url = self._object_value(node, FOAF.homepage)
                ctype_string = "Person" if contacttype == FOAF.Person else "Organization"

                # if the contact has items on top-level, the name has no _name suffix
                name_key = prefix + "_name" if extras_only else prefix

                ds_utils.insert(dataset_dict, name_key, name, extras_only)
                ds_utils.insert(dataset_dict, prefix + "_email", email, extras_only)
                ds_utils.insert(dataset_dict, prefix + "_url", url, True)
                ds_utils.insert(dataset_dict, prefix + "_contacttype", ctype_string, True)

    def _add_maintainer_field(self, dataset_dict, contact, field, _type):
        contact_item = self._object_value(contact, _type)
        ds_utils.insert(dataset_dict, 'maintainer_' + field, contact_item, True)

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

        # Simple additional fields
        for key, predicate in (
               ('qualityProcessURI', DCATDE.qualityProcessURI),
               ('metadata_original_html', DCAT.landingPage),
               ('politicalGeocodingLevelURI', DCATDE.politicalGeocodingLevelURI),
               ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                ds_utils.insert_new_extras_field(dataset_dict, key, value)

        # List fields
        for key, predicate, in (
               ('contributorID', DCATDE.contributorID),
               ('politicalGeocodingURI', DCATDE.politicalGeocodingURI),
               ('legalbasisText', DCATDE.legalbasisText),
               ('geocodingText', DCATDE.geocodingText),
               ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                ds_utils.insert_new_extras_field(dataset_dict, key, json.dumps(values))

        self._parse_contact(dataset_dict, dataset_ref, DCATDE.originator, 'originator', True)
        self._parse_contact(dataset_dict, dataset_ref, DCATDE.maintainer, 'maintainer', False)
        self._parse_contact(dataset_dict, dataset_ref, DCT.contributor, 'contributor', True)
        self._parse_contact(dataset_dict, dataset_ref, DCT.creator, 'author', False)

        # dcat:contactPoint
        # TODO: dcat-ap adds the values to extras.contact_... . Maybe better than maintainer?
        contact = self._object(dataset_ref, DCAT.contactPoint)
        self._add_maintainer_field(dataset_dict, contact, 'url', VCARD.hasURL)

        contact_tel = self._object_value(contact, VCARD.hasTelephone)
        if contact_tel:
            ds_utils.insert(dataset_dict, 'maintainer_tel', self._without_tel(contact_tel), True)

        self._add_maintainer_field(dataset_dict, contact, 'street', VCARD.hasStreetAddress)
        self._add_maintainer_field(dataset_dict, contact, 'city', VCARD.hasLocality)
        self._add_maintainer_field(dataset_dict, contact, 'zip', VCARD.hasPostalCode)
        self._add_maintainer_field(dataset_dict, contact, 'country', VCARD.hasCountryName)

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

        # Add additional distribution fields
        for distribution in self.g.objects(dataset_ref, DCAT.distribution):
            for resource_dict in dataset_dict.get('resources', []):
                # Match distribution in graph and distribution in ckan-dict
                if unicode(distribution) == resource_uri(resource_dict):
                    for key, predicate in (
                            ('licenseAttributionByText', DCATDE.licenseAttributionByText),
                            ('plannedAvailability', DCATDE.plannedAvailability)
                    ):
                        value = self._object_value(distribution, predicate)
                        if value:
                            ds_utils.insert_resource_extra(resource_dict, key, value)

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
            ('politicalGeocodingLevelURI', DCATDE.politicalGeocodingLevelURI, None, URIRef)
        ]
        self._add_triples_from_dict(dataset_dict, dataset_ref, items)

        # Additional Lists
        items = [
            ('contributorID', DCATDE.contributorID, None, Literal),
            ('politicalGeocodingURI', DCATDE.politicalGeocodingURI, None, URIRef),
            ('legalbasisText', DCATDE.legalbasisText, None, Literal),
            ('geocodingText', DCATDE.geocodingText, None, Literal)
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
            contact_point = next(g.objects(dataset_ref, DCAT.contactPoint))
            self._add_triple_from_dict(dataset_dict, contact_point, VCARD.hasURL, 'maintainer_url',
                                       _type=URIRef)

        # add maintainer_tel to contact_point
        maintainer_tel = self._get_dataset_value(dataset_dict, 'maintainer_tel')
        if maintainer_tel:
            contact_point = next(g.objects(dataset_ref, DCAT.contactPoint))
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
                contact_point = next(g.objects(dataset_ref, DCAT.contactPoint))
                g.add((contact_point, vcard_mapping[vc_name], Literal(vcard_fld)))

        # Groups
        groups = self._get_dataset_value(dataset_dict, 'groups')
        for group in groups:
            group_name_in_dict = group['name']
            if group_name_in_dict:
                value_to_add = self._removeWhitespaces(group_name_in_dict)
                if value_to_add:
                    g.add((dataset_ref, DCAT.theme, URIRef(dcat_theme_prefix + value_to_add.upper())))

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
