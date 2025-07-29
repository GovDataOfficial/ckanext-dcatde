#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Various utilities for harvesters.

For instance, they provide functions to rename datasets before they get deleted.
"""
import json
import logging
import uuid

from dateutil.parser import parse as parse_date
import pytz
from ckan import model
from ckan.model import Session, PACKAGE_NAME_MAX_LENGTH
import ckan.plugins as p
from ckanext.dcatde.extras import Extras
from ckanext.harvest.model import HarvestObject, HarvestSource

LOGGER = logging.getLogger(__name__)

# TODO: class methods from ckanext-govdatade. Refactor such that they only occur here

DEFAULT_PRIORITY = 0
NAME_RANDOM_STRING_LENGTH = 5
NAME_DELETED_SUFFIX = "-deleted"
NAME_MAX_LENGTH = PACKAGE_NAME_MAX_LENGTH - NAME_RANDOM_STRING_LENGTH - len(NAME_DELETED_SUFFIX)
EXTRAS_KEY_DCT_IDENTIFIER = 'identifier'
EXTRAS_KEY_DCT_MODIFIED = 'modified'


class HarvestUtils(object):

    '''The class with utility functions for the harvesting process.'''

    @staticmethod
    def build_context():
        """
        Builds a context dictionary.
        """
        return {
            'model': model,
            'session': Session,
            'user': u'harvest',  # TODO: this is configurable now, see _get_user_name of base harvester
            'api_version': 1,
            'ignore_auth': True
        }

    @staticmethod
    def create_new_name_for_deletion(name):
        """
        Creates new name by adding suffix "-deleted" and
        random string to the given name
        """
        random_suffix = str(uuid.uuid4())[:NAME_RANDOM_STRING_LENGTH]
        new_name = name[:NAME_MAX_LENGTH]
        return new_name + NAME_DELETED_SUFFIX + random_suffix

    @staticmethod
    def rename_datasets_before_delete(deprecated_package_dicts):
        """
        Renames the given packages to avoid name conflicts with
        deleted packages.
        """

        package_update = p.toolkit.get_action('package_update')
        for package_dict in deprecated_package_dicts:
            context = HarvestUtils.build_context()
            package_id = package_dict['id']
            package_dict['name'] = HarvestUtils.create_new_name_for_deletion(package_dict.get('name', ''))
            # Update package
            try:
                package_update(context, package_dict)
            except Exception as exception:
                LOGGER.error(u'Unable updating package %s: %s', package_id, exception)

    @staticmethod
    def delete_packages(package_ids):
        """
        Deletes the packages belonging to the given package ids.
        Returns the package IDs of the deleted packages.
        """

        deleted_package_ids = []
        package_delete = p.toolkit.get_action('package_delete')
        for to_delete_id in package_ids:
            context = HarvestUtils.build_context()
            try:
                package_delete(context, {'id': to_delete_id})
                deleted_package_ids.append(to_delete_id)
            except Exception as exception:
                LOGGER.error(
                    u'Unable to delete package with id %s: %s',
                    to_delete_id,
                    exception
                )
        return deleted_package_ids

    @staticmethod
    def rename_delete_dataset_with_id(package_id):
        """
        Deletes the package with package_id. Before deletion, the package is renamed to avoid
        conflicts when adding new packages. If the renaming isn't successful the dataset will be
        deleted anyway.
        """
        context = HarvestUtils.build_context()
        package_dict = p.toolkit.get_action('package_show')(context, {'id': package_id})
        # rename and delete the package
        HarvestUtils.rename_datasets_before_delete([package_dict])
        _mark_harvest_objects_as_not_current([package_id])
        HarvestUtils.delete_packages([package_id])

    @staticmethod
    def compare_harvester_priorities(local_harvester_config, remote_harvester_config):
        """
        Reads the priorities from the harvester-configs and compares them. If the local dataset
        has a higher priority the remote dataset won't be imported.
        """
        local_priority = DEFAULT_PRIORITY
        remote_priority = DEFAULT_PRIORITY
        if "priority" in local_harvester_config:
            local_priority = _parse_priority(local_harvester_config["priority"])
        if "priority" in remote_harvester_config:
            remote_priority = _parse_priority(remote_harvester_config["priority"])

        if remote_priority > local_priority:
            return True
        return False # skip import

    @staticmethod
    def handle_duplicates(harvest_object):
        '''
        Checks if the dataset of a harvest_object already exists. If so then check which dataset to keep.
        Returns True if the remote dataset should be imported, otherwise False.
        '''
        harvest_object_content = harvest_object.content
        harvester_title = harvest_object.source.title
        method_prefix = 'handle_duplicates: '

        remote_dataset = json.loads(harvest_object_content)
        remote_dataset_extras = Extras(remote_dataset['extras'])
        remote_dataset_name = remote_dataset.get('name', '')

        has_orig_id = remote_dataset_extras.key(EXTRAS_KEY_DCT_IDENTIFIER)
        if has_orig_id:
            orig_id = remote_dataset_extras.value(EXTRAS_KEY_DCT_IDENTIFIER)
            # remote dataset contains identifier
            if orig_id:
                try:
                    # Search for other datasets with the same identifier
                    query = model.Session.query(model.Package.id, model.Package.metadata_modified,
                                                model.PackageExtra.value, HarvestObject.harvest_source_id) \
                        .join(HarvestObject, HarvestObject.package_id == model.Package.id) \
                        .join(model.PackageExtra, model.PackageExtra.package_id == model.Package.id) \
                        .filter(model.Package.state == 'active') \
                        .filter(model.PackageExtra.state == 'active') \
                        .filter(model.PackageExtra.key == 'modified') \
                        .filter(model.Package.id.in_(
                            model.Session.query(model.PackageExtra.package_id)
                            .filter(model.PackageExtra.key == 'identifier')
                            .filter(model.PackageExtra.value == orig_id)
                        ))

                    if remote_dataset_extras.key('guid'):
                        # Add filter which excludes datasets with the same guid as the remote dataset
                        query = query.filter(model.Package.id.in_(
                            model.Session.query(model.PackageExtra.package_id)
                            .filter(model.PackageExtra.key == 'guid')
                            .filter(model.PackageExtra.value != remote_dataset_extras.value('guid'))
                        ))

                    local_search_result = {'count': query.count(), 'results': []}
                    for package_id, metadata_modified, modified, harvest_src_id in query:
                        extras = []
                        if modified:
                            extras.append({'key': 'modified', 'value': modified})
                        if harvest_src_id:
                            extras.append({'key': 'harvest_source_id', 'value': harvest_src_id})
                        local_search_result['results'].append({
                            'id': package_id,
                            'metadata_modified': metadata_modified,
                            'extras': extras
                        })

                    if local_search_result['count'] == 0:
                        # no other dataset with the same identifier was found, import accepted
                        LOGGER.debug(u'[%s] %sDid not find any existing dataset in the database with ' \
                                     u'Identifier %s. Import accepted for dataset %s.',
                                     harvester_title, method_prefix, orig_id, remote_dataset_name)
                        return True

                    # The dataset already exists. Check if the remote dataset should be imported.
                    return HarvestUtils.handle_datasets_with_same_id(harvester_title, orig_id,
                            remote_dataset_name, remote_dataset_extras, local_search_result,
                        harvest_object.source.config)
                except Exception as exception:
                    LOGGER.error(exception)
            else:
                LOGGER.debug(u'[%s] %sNo original id in field identifier found. Import accepted for ' \
                             u'dataset %s.', harvester_title, method_prefix, remote_dataset_name)
                return True
        else:
            LOGGER.debug(u'[%s] %sNo field identifier found. Import accepted for dataset %s.',
                         harvester_title, method_prefix, remote_dataset_name)
            return True
        return False

    @staticmethod
    def compare_duplicates(remote_is_latest, harvester_title, local_search_result, latest_local_dataset,
                            remote_dataset_extras, harvest_source_config):
        '''
        Compares local dataset(s) and the remote dataset to see which one should be kept.
        Checks for modified dates and if they are the same checks for priority.
        Returns a triple (remote_is_latest, local_dataset_has_modified, priority_checked) and
        remote_is_latest=True if the remote should be imported.
        '''
        method_prefix = 'compare_duplicates: '
        local_dataset_has_modified = False
        priority_checked = False
        # compare modified date with all local datasets
        for local_dataset in local_search_result['results']:
            local_dataset_extras = Extras(local_dataset['extras'])

            if local_dataset_extras.key(EXTRAS_KEY_DCT_MODIFIED):
                local_dataset_has_modified = True
                # notice the local dataset with the latest date
                _set_or_update_latest_dataset(
                    latest_local_dataset,
                    local_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED),
                    local_dataset['id'])
                # compare dct:modified if remote and local dataset contain the field
                # "modified" and remote dataset is still not detected as older
                if remote_is_latest and remote_dataset_extras.key(EXTRAS_KEY_DCT_MODIFIED):
                    remote_dt = _parse_date(remote_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED))
                    local_dt = _parse_date(local_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED))
                    if remote_dt < local_dt:
                        # remote dataset is older: keep local
                        LOGGER.debug(u'[%s] %sFound a newer dataset in CKAN: skip import.',
                                        harvester_title, method_prefix)
                        remote_is_latest = False
                    elif remote_dt == local_dt:
                        # same timestamp for both: check priority then
                        LOGGER.debug(u'[%s] %sCompare priorities of datasets.', harvester_title,
                            method_prefix)
                        harvest_source_object = _get_harvester_config_from_db(local_dataset_extras.value(
                            "harvest_source_id"))
                        # continue if for some reason harvester-source is not available
                        if harvest_source_object:
                            priority_checked = True
                            remote_is_latest = HarvestUtils.compare_harvester_priorities(
                                json.loads(harvest_source_object.config),
                                json.loads(harvest_source_config))
                    if not remote_is_latest:
                        # remote dataset should not be imported
                        break
        return remote_is_latest, local_dataset_has_modified, priority_checked

    @staticmethod
    def handle_datasets_with_same_id(harvester_title, orig_id, remote_dataset_name,
                                    remote_dataset_extras, local_search_result, harvest_source_config):
        '''
        Checks if the remote or the local dataset should be kept. Delete the other dataset(s).
        Returns True if remote is the latest one and should be imported, otherwise False.
        '''
        method_prefix = 'handle_datasets_with_same_id: '
        try:
            # other dataset with the same identifier was found
            LOGGER.debug(u'[%s] %sFound duplicate entries with Identifier %s for dataset %s.',
                            harvester_title, method_prefix, orig_id, remote_dataset_name)
            remote_is_latest = True
            latest_local_dataset = {}
            if not remote_dataset_extras.key(EXTRAS_KEY_DCT_MODIFIED):
                remote_is_latest = False

            remote_is_latest, local_dataset_has_modified, priority_checked = HarvestUtils.compare_duplicates(
                remote_is_latest, harvester_title, local_search_result, latest_local_dataset,
                remote_dataset_extras, harvest_source_config)

            if remote_is_latest:
                # Import accepted. Delete all local datasets with the same identifier.
                LOGGER.debug(u'[%s] %sRemote dataset with Identifier %s is the latest. '\
                                u'Modified date: %s. Import accepted for dataset %s.',
                                harvester_title, method_prefix, orig_id,
                                remote_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED),
                                remote_dataset_name)
                packages_deleted = _delete_packages_keep(local_search_result['results'])
                LOGGER.debug(u'[%s]: %sDeleted packages: %s', harvester_title,
                             method_prefix, ','.join(packages_deleted))
                return True
            elif local_dataset_has_modified:
                # Skip import. Delete local datasets, but keep the dataset with latest date in
                # the field "modified".
                LOGGER.info(u'[%s] %sRemote dataset with Identifier %s is NOT the latest. '\
                            u'Modified date: %s. Keep local dataset with ' \
                            u'latest date in field "modified". Priority checked: %s Skipping import for '\
                            u'dataset %s!',
                            harvester_title, method_prefix, orig_id,
                            remote_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED, 'n/a'), priority_checked,
                            remote_dataset_name)
                packages_deleted = _delete_packages_keep(
                    local_search_result['results'], latest_local_dataset)
                LOGGER.debug(u'[%s] %sDeleted packages: %s', harvester_title, method_prefix,
                             ','.join(packages_deleted))
            else:
                # Skip import, because remote dataset and no other local dataset contains the
                # field "modified". Delete local datasets, but keep the dataset last modified in
                # database.
                LOGGER.info(
                    u'[%s] %sFound duplicate entries with the value "%s" in field "identifier", '\
                    u'but remote and local datasets does not contain a modified date. ' \
                    u'Keep local dataset last modified in database. Skipping import for %s!',
                    harvester_title, method_prefix, orig_id, remote_dataset_name)
                last_modified_local_dataset = {}
                for local_dataset in local_search_result['results']:
                    # notice the local dataset with the latest date
                    _set_or_update_latest_dataset(
                        last_modified_local_dataset,
                        local_dataset.get('metadata_modified', None),
                        local_dataset['id'])
                packages_deleted = _delete_packages_keep(
                    local_search_result['results'], last_modified_local_dataset)
                LOGGER.debug(u'[%s] %sDeleted packages: %s', harvester_title, method_prefix,
                             ','.join(packages_deleted))
        except Exception as exception:
            LOGGER.error(exception)
        return False

def _get_harvester_config_from_db(harvester_source_id):
    '''
    Searches for a HarvestSource by a harvester source id and returns it.
    '''
    return model.Session.query(HarvestSource).filter(HarvestSource.id == harvester_source_id).first()


def _mark_harvest_objects_as_not_current(package_ids_to_delete):
    '''
    Marks harvest objects with the given package ids as not current.
    '''
    model.Session.query(HarvestObject) \
                .filter(HarvestObject.current.is_(True)) \
                .filter(HarvestObject.package_id.in_(package_ids_to_delete)) \
                .update({'current': False}, False)


def _delete_packages_keep(local_dataset_list, dataset_to_keep=None):
    '''
    Deletes all packages within the given list, except the package with the ID in "dataset_to_keep".
    '''
    package_ids_to_delete = set()
    for local_dataset in local_dataset_list:
        if dataset_to_keep is None or 'id' not in dataset_to_keep or \
                local_dataset['id'] != dataset_to_keep['id']:
            package_ids_to_delete.add(local_dataset['id'])

    deleted_package_ids = []
    if len(package_ids_to_delete) > 0:
        _mark_harvest_objects_as_not_current(package_ids_to_delete)
        deleted_package_ids = HarvestUtils.delete_packages(package_ids_to_delete)
    return deleted_package_ids


def _set_or_update_latest_dataset(latest_local_dataset, modified_date_string, dataset_id):
    '''
    Compares the date string with the date string in "latest_local_dataset" dict and update the date and ID if
    the date is newer than the existent date.
    '''
    try:
        modified_date = _parse_date(modified_date_string)
        if modified_date is not None and \
                ('date' not in latest_local_dataset \
                 or modified_date > latest_local_dataset['date']):
            latest_local_dataset['id'] = dataset_id
            latest_local_dataset['date'] = modified_date
    except Exception as ex:
        # do nothing
        LOGGER.debug(u'Ignoring unexpected error while comparing and updating latest date. Details: %s', ex)


def _parse_date(date_string):
    '''
    Parses a date string to a date object and adds the time zone UTC if no time zone info exists.
    '''
    date_obj = parse_date(date_string)
    if date_obj.tzinfo is None:
        date_obj = date_obj.replace(tzinfo=pytz.UTC)
    return date_obj


def _parse_priority(value):
    '''
    Parse the value to int. Return Default value if not possible.
    '''
    try:
        return int(value)
    except ValueError:
        method_prefix = "parsePriority: "
        LOGGER.warning(u'[%s] Parsing an invalid priority: " %s ". Use default priority.', method_prefix, value)
        return DEFAULT_PRIORITY
