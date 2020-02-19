#!/usr/bin/python
# -*- coding: utf8 -*-
import time
import json
import logging
import pylons

from ckan import plugins as p
from ckan import model
from ckanext.dcat.harvesters.rdf import DCATRDFHarvester
from ckanext.dcat.interfaces import IDCATRDFHarvester
from ckanext.dcatde.dataset_utils import set_extras_field
from ckanext.dcatde.harvesters.harvest_utils import HarvestUtils
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra

LOGGER = logging.getLogger(__name__)

CONFIG_PARAM_HARVESTED_PORTAL = 'harvested_portal'
CONFIG_PARAM_RESOURCES_REQUIRED = 'resources_required'
EXTRA_KEY_HARVESTED_PORTAL = 'metadata_harvested_portal'
RES_EXTRA_KEY_LICENSE = 'license'


class DCATdeRDFHarvester(DCATRDFHarvester):

    p.implements(IDCATRDFHarvester)

    # -- begin IDCATRDFHarvester implementation --
    def before_download(self, url, harvest_job):
        return url, []

    def update_session(self, session):
        # FIXME: use verify=False to allow harvesting with Python < 2.7.9. Remove after Python was upgraded.
        session.verify = False
        return session

    def after_download(self, content, harvest_job):
        return content, []

    def before_update(self, harvest_object, dataset_dict, temp_dict):
        pass

    def after_update(self, harvest_object, dataset_dict, temp_dict):
        return None

    def before_create(self, harvest_object, dataset_dict, temp_dict):
        pass

    def after_create(self, harvest_object, dataset_dict, temp_dict):
        return None
    # -- end IDCATRDFHarvester implementation --

    def info(self):
        return {
            'name': 'dcatde_rdf',
            'title': 'DCAT-AP.de RDF Harvester',
            'description': 'Harvester for DCAT-AP.de datasets from an RDF graph'
        }

    def _get_portal_from_config(self, source_config):
        if source_config:
            return json.loads(source_config).get(CONFIG_PARAM_HARVESTED_PORTAL)

        return ''

    def _get_resources_required_config(self, source_config):
        if source_config:
            return json.loads(source_config).get(CONFIG_PARAM_RESOURCES_REQUIRED, False)

        return False

    def _get_fallback_license(self):
        fallback = pylons.config.get('ckanext.dcatde.harvest.default_license',
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

        return object_ids

    def _amend_package(self, package, pkg_guid, portal, harvester_name):
        '''
        Amend package information.
        '''
        if 'extras' not in package:
            package['extras'] = []

        set_extras_field(package, EXTRA_KEY_HARVESTED_PORTAL, portal)

        # ensure all resources have a license
        for resource in package.get('resources', []):
            if RES_EXTRA_KEY_LICENSE not in resource:
                # pkg_guid is needed because GUID is not set in package dict
                LOGGER.info(u'{3}: No license for resource {0} of package {1} (GUID {2}). '\
                            u'Adding default value.'.format(
                                resource.get('uri', ''), package.get('name', ''), pkg_guid, harvester_name)
                           )
                resource[RES_EXTRA_KEY_LICENSE] = self._get_fallback_license()

    def _skip_datasets_without_resource(self, harvest_object, package):
        '''
        Checks if resources are present when configured and the dataset not already exists.
        '''
        if (self._get_resources_required_config(harvest_object.source.config)\
             and not package.get('resources')):
            skip_notice = ''
            if not self._read_datasets_from_db(harvest_object.guid):
                skip_notice = ' Skipping dataset.'
            # write details to log
            LOGGER.info(u'{0}: Resources are required, but dataset {1} (GUID {2}) has none.{3}'.format(
                harvest_object.source.title, package.get('name', ''), harvest_object.guid, skip_notice))
            if skip_notice:
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
            HarvestUtils.rename_delete_dataset_with_id(harvest_object.package_id)
            LOGGER.info(u'Deleted package {0} with guid {1}'.format(harvest_object.package_id,
                                                                    harvest_object.guid))
            return True

        package = json.loads(harvest_object.content)

        # skip if resources are not present when configured
        if self._skip_datasets_without_resource(harvest_object, package):
            # do not include details in error such that they get summarized in the UI
            self._save_object_error(
                'Dataset has no resources, but they are required by config. Skipping.',
                harvest_object, 'Import'
                )
            return False

        # set custom field and perform other fixes on the data
        portal = self._get_portal_from_config(harvest_object.source.config)
        self._amend_package(package, harvest_object.guid, portal, harvest_object.source.title)
        harvest_object.content = json.dumps(package)

        import_dataset = HarvestUtils.handle_duplicates(harvest_object.content)
        if import_dataset:
            return super(DCATdeRDFHarvester, self).import_stage(harvest_object)
        else:
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
                if not isinstance(harvested_portal, basestring):
                    raise ValueError('%s must be a string' % CONFIG_PARAM_HARVESTED_PORTAL)
            else:
                raise KeyError('%s is not set in config.' % CONFIG_PARAM_HARVESTED_PORTAL)
        else:
            raise ValueError('The parameter %s has to be set in the configuration.' % \
                             CONFIG_PARAM_HARVESTED_PORTAL)

        return cfg
