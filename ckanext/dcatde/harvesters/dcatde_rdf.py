#!/usr/bin/python
# -*- coding: utf8 -*-
import time

from ckan import model
from ckanext.dcat.harvesters.rdf import DCATRDFHarvester
from ckanext.dcatde.dataset_utils import set_extras_field
from ckanext.dcatde.harvesters.harvest_utils import HarvestUtils
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
import json
import logging

LOGGER = logging.getLogger(__name__)

CONFIG_PARAM_HARVESTED_PORTAL = 'harvested_portal'
EXTRA_KEY_HARVESTED_PORTAL = 'metadata_harvested_portal'


class DCATdeRDFHarvester(DCATRDFHarvester):

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
        guids_to_delete = set(guids_in_db) - set(guids_in_source)

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

    def amend_package(self, package, portal):
        if 'extras' not in package:
            package['extras'] = []

        set_extras_field(package, EXTRA_KEY_HARVESTED_PORTAL, portal)

    def import_stage(self, harvest_object):
        # override delete logic
        status = self._get_object_extra(harvest_object, 'status')
        if status == 'delete':
            HarvestUtils.rename_delete_dataset_with_id(harvest_object.package_id)
            LOGGER.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id,
                                                                   harvest_object.guid))
            return True

        portal = self._get_portal_from_config(harvest_object.source.config)

        # set custom field
        package = json.loads(harvest_object.content)
        self.amend_package(package, portal)
        harvest_object.content = json.dumps(package)
        import_dataset = HarvestUtils.handle_duplicates(harvest_object.content)
        if import_dataset:
            return super(DCATdeRDFHarvester, self).import_stage(harvest_object)
        else:
            self._save_object_error('Skipping importing dataset %s, because of duplicate detection!' %
                                    (package.get('name', '')), harvest_object, 'Import')
            return False

    def validate_config(self, source_config):
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
