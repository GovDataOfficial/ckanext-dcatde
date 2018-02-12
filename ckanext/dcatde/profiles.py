from rdflib.namespace import Namespace, RDF, SKOS
from rdflib import URIRef, BNode, Literal

from ckanext.dcat.profiles import RDFProfile
from ckanext.dcat.utils import resource_uri

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

class DCATdeProfile(RDFProfile):
    """ DCAT-AP.de Profile extension """

    def _add_contact(self, dataset_dict, dataset_ref, predicate, prefix):
        """ Adds a Contact with name, email and url """
        if any([
                self._get_dataset_value(dataset_dict, prefix),
                self._get_dataset_value(dataset_dict, prefix+'_name')
        ]):
            new_node = BNode()

            # FOAF.Person and FOAF.Organization are possible, Organization as default.
            contacttype = self._get_dataset_value(dataset_dict,
                                                  prefix+'_contacttype', "Organization")
            if contacttype == "Person":
                self.g.add((new_node, RDF.type, FOAF.Person))
            else:
                self.g.add((new_node, RDF.type, FOAF.Organization))

            self.g.add((dataset_ref, predicate, new_node))

            items = [
                (prefix, FOAF.name, [prefix+'_name'], Literal),
                (prefix+'_email', FOAF.mbox, None, Literal),
                (prefix+'_url', FOAF.homepage, None, URIRef)
            ]
            self._add_triples_from_dict(dataset_dict, new_node, items)


    def parse_dataset(self, dataset_dict, dataset_ref):
        """ Transforms DCAT-AP.de-Data to CKAN-Dictionary """
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
            g.add((contact_point, VCARD.hasURL, URIRef(maintainer_url)))

        # add maintainer_tel to contact_point
        maintainer_tel = self._get_dataset_value(dataset_dict, 'maintainer_tel')
        if maintainer_tel:
            contact_point = next(g.objects(dataset_ref, DCAT.contactPoint))
            g.add((contact_point, VCARD.hasTelephone, URIRef("tel:"+str(maintainer_tel))))

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
        # groups = self._get_dataset_value(dataset_dict, 'groups')
        # for group in groups:
        #     g.add((dataset_ref, DCAT.theme, Literal(dcat_theme_prefix + group['name'].upper())))

        # Categories
        categories = self._get_dataset_value(dataset_dict, 'dcat_ap_eu_data_category')
        if categories is not None:
            for category in categories:
                g.add((dataset_ref, DCAT.theme, Literal(dcat_theme_prefix + category)))

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
