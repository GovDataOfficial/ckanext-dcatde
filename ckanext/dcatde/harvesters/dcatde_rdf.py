#!/usr/bin/python
# -*- coding: utf8 -*-
'''
DCAT-AP.de RDF Harvester module.
'''
import json
import logging
import time

from SPARQLWrapper.SPARQLExceptions import SPARQLWrapperException
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import FOAF
from ckan.lib.base import model
from ckan import plugins as p
from ckan.logic import UnknownValidator
from ckan.plugins import toolkit
from ckanext.dcat.exceptions import RDFParserException
from ckanext.dcat.harvesters.rdf import DCATRDFHarvester
from ckanext.dcat.interfaces import IDCATRDFHarvester
from ckanext.dcat.processors import RDFParser
from ckanext.dcatde.dataset_utils import set_extras_field, EXTRA_KEY_HARVESTED_PORTAL
from ckanext.dcatde.harvesters.harvest_utils import HarvestUtils
from ckanext.dcatde.migration.util import load_json_mapping
from ckanext.dcatde.triplestore.fuseki_client import FusekiTriplestoreClient
from ckanext.dcatde.triplestore.sparql_query_templates import GET_DATASET_BY_URI_SPARQL_QUERY, \
    GET_URIS_FROM_HARVEST_INFO_QUERY
from ckanext.dcatde.validation.shacl_validation import ShaclValidator
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckan.config.environment import load_environment
from ckan.cli import load_config as _get_config

load_environment(_get_config())

LOGGER = logging.getLogger(__name__)

CONFIG_PARAM_HARVESTED_PORTAL = 'harvested_portal'
CONFIG_PARAM_RESOURCES_REQUIRED = 'resources_required'
RES_EXTRA_KEY_LICENSE = 'license'


class DCATdeRDFHarvester(DCATRDFHarvester):
    """ DCAT-AP.de RDF Harvester """

    p.implements(IDCATRDFHarvester)

    # -- begin IDCATRDFHarvester implementation --
    # pylint: disable=C0111,W0613,R0201,C0103
    def before_download(self, url, harvest_job):
        return url, []

    def update_session(self, session):
        return session

    def after_download(self, content, harvest_job):
        return content, []

    def after_parsing(self, rdf_parser, harvest_job):
        """ Insert harvested data into triplestore and validate the data """

        error_messages = []
        if rdf_parser and self.triplestore_client.is_available():
            LOGGER.debug(u'Start updating triplestore...')

            source_dataset = model.Package.get(harvest_job.source.id)
            org_available = True
            if not source_dataset or not hasattr(source_dataset, 'owner_org'):
                org_available = False
                LOGGER.warn(u'There is no organization specified in the harvest source. SHACL validation ' \
                            u'will be deactivated!')

            for uri in rdf_parser._datasets():
                LOGGER.debug(u'Process URI: %s', uri)
                try:
                    self._delete_dataset_in_triplestore_by_uri(uri, source_dataset)

                    triples = rdf_parser.g.query(GET_DATASET_BY_URI_SPARQL_QUERY % {'uri': uri})

                    if triples:
                        graph = Graph()
                        for triple in triples:
                            graph.add(triple)
                        rdf_graph = graph.serialize(format="turtle")

                        self.triplestore_client.create_dataset_in_triplestore(rdf_graph, uri)

                        if org_available:
                            # save harvesting info
                            harvest_graph = Graph()
                            harvest_graph.bind("foaf", FOAF)
                            harvest_graph.add((URIRef(uri), FOAF.knows, Literal(source_dataset.owner_org)))
                            rdf_harvest_graph = harvest_graph.serialize(format="xml")
                            self.triplestore_client.create_dataset_in_triplestore_harvest_info(
                                rdf_harvest_graph, uri)
                            # SHACL Validation
                            self._validate_dataset_rdf_graph(uri, rdf_graph, source_dataset)
                    else:
                        LOGGER.warn(u'Could not find triples to URI %s. Updating is not possible.', uri)
                except SPARQLWrapperException as exception:
                    LOGGER.error(u'Unexpected error while deleting dataset with URI %s from TripleStore: %s',
                                 uri, exception)
                    error_messages.append(u'Error while deleting dataset from TripleStore: %s' % exception)
                except Exception as exception:
                    LOGGER.warn(u'Unexpected error or error while graph serialization: %s. Skipping ' \
                                u'dataset with URI %s.', exception, uri)
                    error_messages.append(u'Unexpected error or error while graph serialization: %s' \
                                          % exception)
            LOGGER.debug(u'Finished updating triplestore.')

        return rdf_parser, error_messages

    def before_update(self, harvest_object, dataset_dict, temp_dict):
        pass

    def after_update(self, harvest_object, dataset_dict, temp_dict):
        return None

    def before_create(self, harvest_object, dataset_dict, temp_dict):
        pass

    def after_create(self, harvest_object, dataset_dict, temp_dict):
        return None

    def update_package_schema_for_create(self, package_schema):
        try:
            package_schema['maintainer_email'].remove(self.email_validator)
            package_schema['author_email'].remove(self.email_validator)
        except ValueError:
            pass
        return package_schema

    def update_package_schema_for_update(self, package_schema):
        try:
            package_schema['maintainer_email'].remove(self.email_validator)
            package_schema['author_email'].remove(self.email_validator)
        except ValueError:
            pass
        return package_schema
    # pylint: enable=C0111,W0613,R0201,C0103
    # -- end IDCATRDFHarvester implementation --

    def __init__(self, name='dcatde_rdf'):
        '''
        Set global parameters from config
        '''
        DCATRDFHarvester.__init__(self)

        self.triplestore_client = FusekiTriplestoreClient()
        self.shacl_validator_client = ShaclValidator()

        self.licenses_upgrade = {}
        license_file = toolkit.config.get('ckanext.dcatde.urls.dcat_licenses_upgrade_mapping')
        if license_file:
            self.licenses_upgrade = load_json_mapping(license_file, "DCAT License upgrade mapping", LOGGER)
        try:
            self.email_validator = toolkit.get_validator('email_validator')
        except UnknownValidator:
            pass

    def info(self):
        return {
            'name': 'dcatde_rdf',
            'title': 'DCAT-AP.de RDF Harvester',
            'description': 'Harvester for DCAT-AP.de datasets from an RDF graph'
        }

    @staticmethod
    def _get_portal_from_config(source_config):
        if source_config:
            return json.loads(source_config).get(CONFIG_PARAM_HARVESTED_PORTAL)

        return ''

    @staticmethod
    def _get_resources_required_config(source_config):
        if source_config:
            return json.loads(source_config).get(CONFIG_PARAM_RESOURCES_REQUIRED, False)

        return False

    @staticmethod
    def _get_fallback_license():
        fallback = toolkit.config.get('ckanext.dcatde.harvest.default_license',
                                     'http://dcat-ap.de/def/licenses/other-closed')
        return fallback

    def _mark_datasets_for_deletion(self, guids_in_source, harvest_job):
        # This is the same as the method in the base class, except that a different query is used.

        object_ids = []

        portal = self._get_portal_from_config(harvest_job.source.config)

        starttime = time.time()
        # Get all previous current guids and dataset ids for this harvested portal independent of
        # the harvest objects. This allows cleaning the harvest data without loosing the
        # dataset mappings.
        # Build a subquery to get all active packages having a GUID first
        # pylint: disable=E1101
        subquery = model.Session.query(model.PackageExtra.value, model.Package.id) \
            .join(model.Package, model.Package.id == model.PackageExtra.package_id)\
            .filter(model.Package.state == model.State.ACTIVE) \
            .filter(model.PackageExtra.state == model.State.ACTIVE) \
            .filter(model.PackageExtra.key == 'guid') \
            .subquery()
        # then get all active packages of the current portal and join with their GUIDs if
        # available (outer join)
        query = model.Session.query(model.Package.id, subquery.c.value) \
            .join(model.PackageExtra, model.PackageExtra.package_id == model.Package.id)\
            .outerjoin(subquery, subquery.c.id == model.Package.id)\
            .filter(model.Package.state == model.State.ACTIVE) \
            .filter(model.PackageExtra.state == model.State.ACTIVE) \
            .filter(model.PackageExtra.key == EXTRA_KEY_HARVESTED_PORTAL) \
            .filter(model.PackageExtra.value == portal)
        # pylint: enable=E1101

        checkpoint_start = time.time()
        guid_to_package_id = {}
        for package_id, guid in query:
            if guid:
                guid_to_package_id[guid] = package_id
            # Also remove all packages without a GUID, use ID as GUID to share logic below
            else:
                guid_to_package_id[package_id] = package_id
        checkpoint_end = time.time()
        LOGGER.debug('Time for query harvest source related datasets : %s',
                     str(checkpoint_end - checkpoint_start))

        guids_in_db = guid_to_package_id.keys()

        # Get objects/datasets to delete (ie in the DB but not in the source)
        guids_in_source_unique = set(guids_in_source)
        guids_in_db_unique = set(guids_in_db)
        LOGGER.debug('guids in source: %s, unique guids in source: %s, '\
                      'guids in db: %s, unique guids in db: %s', len(guids_in_source),
                     len(guids_in_source_unique), len(guids_in_db), len(guids_in_db_unique))
        guids_to_delete = guids_in_db_unique - guids_in_source_unique

        # Create a harvest object for each of them, flagged for deletion
        for guid in guids_to_delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HarvestObjectExtra(key='status',
                                                           value='delete')])

            # Mark the rest of objects for this guid as not current
            model.Session.query(HarvestObject) \
                .filter_by(guid=guid) \
                .update({'current': False}, False)
            obj.save()
            object_ids.append(obj.id)

        endtime = time.time()
        LOGGER.debug('Found %s packages for deletion. Time total: %s', len(guids_to_delete),
                     str(endtime - starttime))

        self._delete_deprecated_datasets_from_triplestore(
            guids_in_source_unique, guids_to_delete, harvest_job)

        return object_ids

    def _amend_package(self, harvest_object):
        '''
        Amend package information.
        '''
        package = json.loads(harvest_object.content)
        if 'extras' not in package:
            package['extras'] = []

        portal = self._get_portal_from_config(harvest_object.source.config)
        set_extras_field(package, EXTRA_KEY_HARVESTED_PORTAL, portal)

        # ensure all resources have a (recent) license
        for resource in package.get('resources', []):
            log_prefix = u'{0}: Resource {1} of package {2} (GUID {3})'.format(
                harvest_object.source.title, resource.get('uri', ''), package.get('name', ''),
                harvest_object.guid
            )

            if resource.get(RES_EXTRA_KEY_LICENSE, '') == '':
                LOGGER.info(log_prefix + u' has no license. Adding default value.')
                resource[RES_EXTRA_KEY_LICENSE] = self._get_fallback_license()
            elif self.licenses_upgrade:
                current_license = resource.get(RES_EXTRA_KEY_LICENSE)
                new_license = self.licenses_upgrade.get(current_license, '')
                if new_license == '':
                    LOGGER.info(log_prefix + u' has a deprecated or unknown license {0}. '\
                        u'Keeping old value.'.format(current_license))
                elif current_license != new_license:
                    LOGGER.info(log_prefix + u' had old license {0}. '\
                        u'Updated value to recent DCAT list.'.format(current_license))
                    resource[RES_EXTRA_KEY_LICENSE] = new_license
        # write changes back to harvest object content
        harvest_object.content = json.dumps(package)

    def _skip_datasets_without_resource(self, harvest_object):
        '''
        Checks if resources are present when configured.
        '''
        package = json.loads(harvest_object.content)
        if (self._get_resources_required_config(harvest_object.source.config)\
             and not package.get('resources')):
            # write details to log
            LOGGER.info(u'%s: Resources are required, but dataset %s (GUID %s) has none. Skipping dataset.',
                        harvest_object.source.title, package.get('name', ''), harvest_object.guid)
            return True
        return False

    def import_stage(self, harvest_object):
        '''
        Import stage for the DCAT-AP.de harvester.
        '''

        LOGGER.debug('In DCATdeRDFHarvester import_stage')

        # override delete logic
        status = self._get_object_extra(harvest_object, 'status')
        if status == 'delete':
            if not harvest_object.package_id:
                LOGGER.warn(u'Harvest object with status delete contains no package id for guid %s',
                            harvest_object.guid)
                return False
            HarvestUtils.rename_delete_dataset_with_id(harvest_object.package_id)
            self._delete_dataset_in_triplestore(harvest_object)
            LOGGER.info(u'Deleted package %s with guid %s', harvest_object.package_id, harvest_object.guid)
            return True

        # skip if resources are not present when configured
        if self._skip_datasets_without_resource(harvest_object):
            info_deleted_local_dataset = ''
            datasets_from_db = self._read_datasets_from_db(harvest_object.guid)
            if datasets_from_db:
                if len(datasets_from_db) == 1:
                    HarvestUtils.rename_delete_dataset_with_id(datasets_from_db[0][0])
                    LOGGER.info(u'Deleted local dataset with GUID %s as harvest object has '\
                                u'no resources.', harvest_object.guid)
                    info_deleted_local_dataset = ' Local dataset without resources deleted.'
                else:
                    LOGGER.warn(
                        u'Not deleting package with GUID %s, because more than one dataset was found!',
                        harvest_object.guid
                    )
                    info_deleted_local_dataset = ' More than one local dataset with the same GUID!'
            # do not include details in error such that they get summarized in the UI
            self._save_object_error(
                'Dataset has no resources, but they are required by config. Skipping.{0}'.format(
                    info_deleted_local_dataset),
                harvest_object, 'Import'
                )
            return False

        # set custom field and perform other fixes on the data
        self._amend_package(harvest_object)

        import_dataset = HarvestUtils.handle_duplicates(harvest_object)
        if import_dataset:
            return super(DCATdeRDFHarvester, self).import_stage(harvest_object)

        self._save_object_error('Skipping importing dataset, because of duplicate detection!',
                                harvest_object, 'Import')
        return False

    def validate_config(self, source_config):
        '''
        Validates additional configuration parameters for DCAT-AP.de harvester.
        '''
        cfg = super(DCATdeRDFHarvester, self).validate_config(source_config)

        if cfg:
            config_obj = json.loads(cfg)
            if CONFIG_PARAM_HARVESTED_PORTAL in config_obj:
                harvested_portal = config_obj[CONFIG_PARAM_HARVESTED_PORTAL]
                if not isinstance(harvested_portal, str):
                    raise ValueError('%s must be a string' % CONFIG_PARAM_HARVESTED_PORTAL)
            else:
                raise KeyError('%s is not set in config.' % CONFIG_PARAM_HARVESTED_PORTAL)
        else:
            raise ValueError('The parameter %s has to be set in the configuration.' % \
                             CONFIG_PARAM_HARVESTED_PORTAL)

        return cfg

    def _delete_dataset_in_triplestore(self, harvest_object):
        '''
        Deletes the package with the given package ID in the triple store.
        '''
        try:
            if self.triplestore_client.is_available():
                package_id = harvest_object.package_id
                LOGGER.debug(u'Start deleting dataset with ID %s from triplestore.', package_id)
                context = {'user': self._get_user_name()}
                rdf = toolkit.get_action('dcat_dataset_show')(context, {'id': package_id})
                rdf_parser = RDFParser()
                rdf_parser.parse(rdf)
                # Should be only one dataset
                uri = next(rdf_parser._datasets(), None)
                source_dataset = model.Package.get(harvest_object.source.id)
                self._delete_dataset_in_triplestore_by_uri(uri, source_dataset)
        except RDFParserException as ex:
            LOGGER.warn(u'Error while parsing the RDF file for dataset with ID %s: %s',
                        package_id, ex)
        except SPARQLWrapperException as ex:
            LOGGER.warn(u'Error while deleting dataset with URI %s from triplestore: %s', uri, ex)

    def _delete_dataset_in_triplestore_by_uri(self, uri, source_dataset):
        '''
        Deletes the package with the given URI in the triple store.
        '''
        if self.triplestore_client.is_available():
            LOGGER.debug(u'Start deleting dataset with URI %s from triplestore.', uri)
            if uri:
                self.triplestore_client.delete_dataset_in_triplestore(uri)
                if source_dataset and hasattr(source_dataset, 'owner_org'):
                    self.triplestore_client.delete_dataset_in_triplestore_mqa(
                        uri, source_dataset.owner_org)
                    self.triplestore_client.delete_dataset_in_triplestore_harvest_info(
                        uri, source_dataset.owner_org)
                LOGGER.debug(u'Successfully deleted dataset with URI %s from triplestore.', uri)
            else:
                LOGGER.debug(u'URI could not determined. Skip deleting.')

    def _validate_dataset_rdf_graph(self, uri, rdf_graph, source_dataset):
        '''
        Validates the package rdf graph with the given URI and saves the validation report in the
        triple store.
        '''
        result = self.shacl_validator_client.validate(
            rdf_graph, uri, source_dataset.owner_org)
        if result:
            self.triplestore_client.create_dataset_in_triplestore_mqa(result, uri)

    def _delete_deprecated_datasets_from_triplestore(self, harvested_uris, uris_db_marked_deleted,
                                                     harvest_job):
        '''
        Check for deprecated datasets (not stored in CKAN) in the triplestore and delete them from all
        datastores.
        '''

        # get owner org
        source_dataset = model.Package.get(harvest_job.source.id)
        if source_dataset and hasattr(source_dataset, 'owner_org'):
            # Read URIs from harvest_info datastore
            owner_org = source_dataset.owner_org
            existing_uris = self._get_existing_dataset_uris_from_triplestore(owner_org)

            # compare existing with harvested URIs to see which URIs were not updated
            existing_uris_unique = set(existing_uris)
            harvested_uris_unique = set(harvested_uris)
            uris_to_be_deleted = (existing_uris_unique - harvested_uris_unique) - set(uris_db_marked_deleted)
            LOGGER.info(u'Found %s harvesting URIs in the triplestore belonging to organization %s ' \
                        u'that are no longer provided.',
                        len(uris_to_be_deleted), owner_org)

            # delete deprecated datasets from triplestore
            for dataset_uri in uris_to_be_deleted:
                LOGGER.info(u'Delete <%s> from all triplestore datastores.', dataset_uri)
                try:
                    self._delete_dataset_in_triplestore_by_uri(dataset_uri, source_dataset)
                except SPARQLWrapperException as ex:
                    LOGGER.warn(u'Error while deleting dataset with URI %s from triplestore: %s',
                                dataset_uri, ex)
        else:
            LOGGER.info(u'Harvest source %s, harvest job %s: "owner_org" NOT found. Cannot retrieve the ' \
                        u'harvested URIs to the harvest source. Deprecated datasets which are not ' \
                        u'stored in CKAN will not be deleted properly from the triplestore.',
                        harvest_job.source.id, harvest_job.id)

    def _get_existing_dataset_uris_from_triplestore(self, owner_org):
        '''
        Requests all URIs from the harvest_info datastore and returns them as a list.
        '''
        existing_uris = []
        try:
            query = GET_URIS_FROM_HARVEST_INFO_QUERY % {'owner_org': owner_org}
            raw_response = self.triplestore_client.select_datasets_in_triplestore_harvest_info(query)
            response = raw_response.convert()
            for res in response["results"]["bindings"]:
                if "s" in res:
                    existing_uris.append(res["s"]["value"])
        except SPARQLWrapperException as exception:
            LOGGER.error(u'Unexpected error while querying harvest info from triplestore: %s', exception)
        return existing_uris
