""" DCAT-AP.de Profile """

import re
import unittest
import rdflib

from rdflib import URIRef
from rdflib.namespace import Namespace

from ckanext.dcat.profiles import EuropeanDCATAPProfile
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
        'locn': LOCN,
        'gsp': GSP,
        'owl': OWL,
        'spdx': SPDX,

        # own extension
        'dcatde': DCATDE
    }

    graph = rdflib.Graph()

    predicate_pattern = re.compile("[a-zA-Z]:[a-zA-Z]")

    def transform_to_key_value(self, source):
        """ convert dictionary entry to ckan-extras-field-format """
        return [{"key": key, "value": source[key]} for key in source]

    def get_value_from_extras(self, extras, key):
        """ retrieves a value from the key-value representation used in extras dict """
        return [x["value"] for x in extras if x["key"] == key][0]

    def assert_list(self, ref, predicate, values):
        """ check for every item of a predicate to exist in the graph """
        for obj in self.graph.objects(ref, predicate):
            if unicode(obj) in values:
                values.remove(unicode(obj))

        self.assertTrue(len(values) == 0, "Not all expected values were found in graph. remaining: "
                        + ", ".join(values))

    def predicate_from_string(self, predicate):
        """ take "dct:title" and transform to DCT.title, to be read by rdflib """
        prefix, name = predicate.split(":")
        return self.namespaces[prefix][name]


    def check_simple_items(self, source, ref, item):
        """ checks the subgraph for different types of items """
        if isinstance(item, dict):  # handle extra-array items
            value = item["value"]
        else:
            value = source[item]

        if isinstance(value, str) and self.predicate_pattern.match(value):
            self.assert_list(ref, self.predicate_from_string(value), [value])

    def assert_contact_info(self, dataset_ref, predicate):
        """ check name, email and url for a given rdf-subelement """
        contact = list(self.graph.objects(dataset_ref, predicate))[0]
        self.assertEqual(len(list(self.graph.objects(contact, self.FOAF.name))), 1,
                         predicate + " name not found")
        self.assertEqual(len(list(self.graph.objects(contact, self.FOAF.mbox))), 1,
                         predicate + " mbox not found")
        self.assertEqual(len(list(self.graph.objects(contact, self.FOAF.homepage))), 1,
                         predicate + " homepage not found")

    def test_graph_from_dataset(self):
        """ test dcat and dcatde profiles """

        dataset_dict = {
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

            "extras": self.transform_to_key_value({
                "contributorID": ["dcatde:contributorID"],
                "qualityProcessURI": "dcatde:qualityProcessURI",
                "documentation": "foaf:page",
                "frequency": "dct:accrualPeriodicity",
                "version_notes": "adms:versionNotes",
                "dcat_type": "dct:type",

                "author_url": "nocheck",

                "maintainer_url": "nocheck",
                "maintainer_tel": "nocheck",
                'maintainer_street': "nocheck",
                'maintainer_city': "nocheck",
                'maintainer_zip': "nocheck",
                'maintainer_country': "nocheck",

                "publisher_name": "nocheck",
                "publisher_email": "nocheck",
                "publisher_url": "nocheck",

                "originator_name": "nocheck",
                "originator_email": "nocheck",
                "originator_url": "nocheck",

                "contributor_name": "nocheck",
                "contributor_email": "nocheck",
                "contributor_url": "nocheck",

                "access_rights": "dct:accessRights",
                "provenance": "dct:provenance",
                "politicalGeocodingLevelURI": "dcatde:politicalGeocodingLevelURI",
                "politicalGeocodingURI": ["dcatde:politicalGeocodingURI"],
                "geocodingText": ["dcatde:geocodingText"],
                "legalbasisText": ["dcatde:legalbasisText"],

                "temporal_start": "2017-07-06T13:08:40",
                "temporal_end": "2017-07-06T13:08:41",

                "spatial": "{\"type\":\"Polygon\",\"coordinates\":[[[8.852920532226562,"+
                           "47.97245599240245],[9.133758544921875,47.97245599240245],"+
                           "[9.133758544921875,48.17249666038475],[8.852920532226562,"+
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

                "extras": self.transform_to_key_value({
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

        dataset_ref = URIRef("http://testuri/")

        dcat = EuropeanDCATAPProfile(self.graph, False)
        dcat.graph_from_dataset(dataset_dict, dataset_ref)

        dcatde = DCATdeProfile(self.graph, False)
        dcatde.graph_from_dataset(dataset_dict, dataset_ref)


        # Assert structure of graph - basic values
        extras = dataset_dict["extras"]

        for key in dataset_dict:
            self.check_simple_items(dataset_dict, dataset_ref, key)

        for key in extras:
            self.check_simple_items(dataset_dict, dataset_ref, key)

        # issued, modified
        self.assertEqual(len(list(self.graph.objects(dataset_ref, self.DCT.issued))), 1,
                         "dct:issued not found")
        self.assertEqual(len(list(self.graph.objects(dataset_ref, self.DCT.modified))), 1,
                         "dct:modified not found")

        # groups, tags
        self.assert_list(dataset_ref, self.DCAT.theme,
                         [self.dcat_theme_prefix + x["name"] for x in dataset_dict["groups"]])
        self.assert_list(dataset_ref, self.DCAT.keyword,
                         [x["name"] for x in dataset_dict["tags"]])

        # author, maintainer, originator, contributor, publisher
        self.assert_contact_info(dataset_ref, self.DCATDE.originator)
        self.assert_contact_info(dataset_ref, self.DCATDE.maintainer)
        self.assert_contact_info(dataset_ref, self.DCT.contributor)
        self.assert_contact_info(dataset_ref, self.DCT.creator)
        self.assert_contact_info(dataset_ref, self.DCT.publisher)

        # contactPoint
        contact_point = next(self.graph.objects(dataset_ref, self.DCAT.contactPoint))
        vcard_attrs = [
            self.VCARD.fn, self.VCARD.hasEmail, self.VCARD.hasURL,
            self.VCARD.hasTelephone, self.VCARD.hasStreetAddress,
            self.VCARD.hasLocality, self.VCARD.hasCountryName,
            self.VCARD.hasPostalCode
        ]
        for v_attr in vcard_attrs:
            self.assertEqual(len(list(self.graph.objects(contact_point, v_attr))), 1,
                             self.DCAT.contactPoint + str(v_attr) + " not found")

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
        self.assert_list(dataset_ref, self.DCT.language,
                         self.get_value_from_extras(extras, "language"))
        self.assert_list(dataset_ref, self.DCT.conformsTo,
                         self.get_value_from_extras(extras, "conforms_to"))
        self.assert_list(dataset_ref, self.ADMS.identifier,
                         self.get_value_from_extras(extras, "alternate_identifier"))
        self.assert_list(dataset_ref, self.DCT.relation,
                         self.get_value_from_extras(extras, "used_datasets"))
        self.assert_list(dataset_ref, self.DCT.hasVersion,
                         self.get_value_from_extras(extras, "has_version"))
        self.assert_list(dataset_ref, self.DCT.isVersionOf,
                         self.get_value_from_extras(extras, "is_version_of"))
        self.assert_list(dataset_ref, self.DCATDE.politicalGeocodingURI,
                         self.get_value_from_extras(extras, "politicalGeocodingURI"))
        self.assert_list(dataset_ref, self.DCATDE.geocodingText,
                         self.get_value_from_extras(extras, "geocodingText"))
        self.assert_list(dataset_ref, self.DCATDE.legalbasisText,
                         self.get_value_from_extras(extras, "legalbasisText"))
        self.assert_list(dataset_ref, self.DCATDE.contributorID,
                         self.get_value_from_extras(extras, "contributorID"))

        # resources
        resource = dataset_dict["resources"][0]
        resource_ref = list(self.graph.objects(dataset_ref, self.DCAT.distribution))[0]
        resource_extras = resource["extras"]

        for key in resource:
            self.check_simple_items(resource, resource_ref, key)

        for key in resource_extras:
            self.check_simple_items(resource, resource_ref, key)

        # size
        self.assertEqual(len(list(self.graph.objects(resource_ref, self.DCAT.byteSize))), 1,
                         self.DCAT.byteSize + " not found")

        # hash
        self.assertEqual(len(list(self.graph.objects(resource_ref, self.SPDX.checksum))), 1,
                         self.SPDX.checksum + " not found")

        # lists
        self.assert_list(resource_ref, self.DCT.language,
                         self.get_value_from_extras(resource_extras, "language"))
        self.assert_list(resource_ref, self.DCT.conformsTo,
                         self.get_value_from_extras(resource_extras, "conforms_to"))
