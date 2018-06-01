#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import logging

from ckan import model
from ckanext.dcat.harvesters.rdf import DCATRDFHarvester
from ckanext.dcatde.dataset_utils import set_extras_field
from ckanext.dcatde.harvesters.harvest_utils import HarvestUtils
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckanext.harvest.harvesters import HarvesterBase



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

        # Get all previous current guids and dataset ids for this harvested portal independent of
        # the harvest objects. This allows cleaning the harvest data without loosing the
        # dataset mappings.
        # Build a subquery to get all the packages of the current portal first
        portal_packages = model.Session.query(model.PackageExtra.package_id.label('id')) \
            .filter(model.PackageExtra.key == EXTRA_KEY_HARVESTED_PORTAL) \
            .filter(model.PackageExtra.value == portal) \
            .subquery()

        # then get the extras.guid for those packages
        query = model.Session.query(model.PackageExtra.value, portal_packages.c.id) \
            .filter(model.PackageExtra.key == 'guid') \
            .filter(model.PackageExtra.package_id == portal_packages.c.id)

        guid_to_package_id = {}
        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

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
