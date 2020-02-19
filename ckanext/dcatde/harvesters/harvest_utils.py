#!/usr/bin/python
# -*- coding: utf8 -*-
"""
Various utilities for harvesters.

For instance, they provide functions to rename datasets before they get deleted.
"""
import json
import logging
import uuid
import pytz

from ckan import model
from ckan.model import Session, PACKAGE_NAME_MAX_LENGTH
import ckan.plugins as p
from dateutil.parser import parse as parse_date
from ckanext.dcatde.extras import Extras


LOGGER = logging.getLogger(__name__)

# TODO: class methods from ckanext-govdatade. Refactor such that they only occur here

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
        Returns the package IDs of the re-named packages.
        """

        renamed_package_ids = []
        package_update = p.toolkit.get_action('package_update')
        for package_dict in deprecated_package_dicts:
            context = HarvestUtils.build_context()
            package_id = package_dict['id']
            package_dict['name'] = HarvestUtils.create_new_name_for_deletion(package_dict.get('name', ''))
            # Update package
            try:
                package_update(context, package_dict)
                renamed_package_ids.append(package_id)
            except Exception as exception:
                LOGGER.error(u'Unable updating package %s: %s', package_id, exception)

        return renamed_package_ids

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
        conflicts when adding new packages.
        """
        context = HarvestUtils.build_context()
        package_dict = p.toolkit.get_action('package_show')(context, {'id': package_id})
        # rename and delete the package (renamed_ids contains the current package ID at most)
        renamed_ids = HarvestUtils.rename_datasets_before_delete([package_dict])
        HarvestUtils.delete_packages(renamed_ids)

    @staticmethod
    def compare_metadata_modified(remote_modified, local_modified):
        '''
        Compares the modified datetimes of the metadata. Returns True if the remote date (first parameter)
        is newer.
        '''
        remote_dt = _parse_date(remote_modified)
        local_dt = _parse_date(local_modified)
        if remote_dt <= local_dt:
            return False

        return True

    @staticmethod
    def handle_duplicates(harvest_object_content):
        '''Compares new dataset with existing and checks, if a dataset should be imported.'''

        method_prefix = 'handle_duplicates: '
        context = HarvestUtils.build_context()

        remote_dataset = json.loads(harvest_object_content)
        remote_dataset_extras = Extras(remote_dataset['extras'])
        remote_dataset_name = remote_dataset.get('name', '')

        has_orig_id = remote_dataset_extras.key(EXTRAS_KEY_DCT_IDENTIFIER)
        if has_orig_id:
            orig_id = remote_dataset_extras.value(EXTRAS_KEY_DCT_IDENTIFIER)
            # remote dataset contains identifier
            if orig_id:
                try:
                    data_dict = {"q": EXTRAS_KEY_DCT_IDENTIFIER + ':"' + orig_id + '"'}
                    # Add filter that local dataset guid is not equal to guid of the remote dataset
                    if remote_dataset_extras.key('guid'):
                        data_dict['fq'] = '-guid:"' + remote_dataset_extras.value('guid') + '"'
                    # search for other datasets with the same identifier
                    local_search_result = p.toolkit.get_action("package_search")(context, data_dict)
                    if local_search_result['count'] == 0:
                        # no other dataset with the same identifier was found, import accepted
                        LOGGER.debug(u'%sDid not find any existing dataset in the database. ' \
                            u'Import accepted for dataset %s.', method_prefix, remote_dataset_name)
                        return True
                    else:
                        # other dataset with the same identifier was found
                        LOGGER.debug(u'%sFound duplicate entries for dataset %s.', method_prefix,
                                     remote_dataset_name)
                        remote_is_latest = True
                        local_dataset_has_modified = False
                        latest_local_dataset = {}
                        if not remote_dataset_extras.key(EXTRAS_KEY_DCT_MODIFIED):
                            remote_is_latest = False

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
                                    remote_is_latest = HarvestUtils.compare_metadata_modified(
                                        remote_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED),
                                        local_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED)
                                    )

                        if remote_is_latest:
                            # Import accepted. Delete all local datasets with the same identifier.
                            LOGGER.debug(u'%sRemote dataset is the latest. Import accepted for dataset %s.',
                                         method_prefix, remote_dataset_name)
                            packages_deleted = _delete_packages_keep(local_search_result['results'])
                            LOGGER.debug(u'%sDeleted packages: %s', method_prefix, ','.join(packages_deleted))
                            return True
                        elif local_dataset_has_modified:
                            # Skip import. Delete local datasets, but keep the dataset with latest date in
                            # the field "modified".
                            LOGGER.info(u'%sRemote dataset is NOT the latest. Keep local dataset with ' \
                                        u'latest date in field "modified". Skipping import for dataset %s!',
                                        method_prefix, remote_dataset_name)
                            packages_deleted = _delete_packages_keep(
                                local_search_result['results'], latest_local_dataset)
                            LOGGER.debug(u'%sDeleted packages: %s', method_prefix, ','.join(packages_deleted))
                        else:
                            # Skip import, because remote dataset and no other local dataset contains the
                            # field "modified". Delete local datasets, but keep the dataset last modified in
                            # database.
                            LOGGER.info(
                                u'%sFound duplicate entries with the value "%s" in field "identifier", but ' \
                                u'remote and local datasets does not contain a modified date. ' \
                                u'Keep local dataset last modified in database. Skipping import for %s!',
                                method_prefix, orig_id, remote_dataset_name)
                            last_modified_local_dataset = {}
                            for local_dataset in local_search_result['results']:
                                # notice the local dataset with the latest date
                                _set_or_update_latest_dataset(
                                    last_modified_local_dataset,
                                    local_dataset.get('metadata_modified', None),
                                    local_dataset['id'])
                            packages_deleted = _delete_packages_keep(
                                local_search_result['results'], last_modified_local_dataset)
                            LOGGER.debug(u'%sDeleted packages: %s', method_prefix, ','.join(packages_deleted))
                except Exception as exception:
                    LOGGER.error(exception)
            else:
                LOGGER.debug(u'%sNo original id in field identifier found. Import accepted for dataset %s.',
                             method_prefix, remote_dataset_name)
                return True
        else:
            LOGGER.debug(u'%sNo field identifier found. Import accepted for dataset %s.',
                         method_prefix, remote_dataset_name)
            return True

        return False


def _delete_packages_keep(local_dataset_list, dataset_to_keep=None):
    '''
    Delete all packages within the given list, except the package with the ID in "dataset_to_keep".
    '''
    package_ids_to_delete = set()
    for local_dataset in local_dataset_list:
        if dataset_to_keep is None or 'id' not in dataset_to_keep or \
                local_dataset['id'] != dataset_to_keep['id']:
            package_ids_to_delete.add(local_dataset['id'])

    packages_deleted = HarvestUtils.delete_packages(package_ids_to_delete)
    return packages_deleted


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
    except Exception as exception:
        # do nothing
        pass


def _parse_date(date_string):
    '''
    Parse a date string to a date object and add the time zone UTC if no time zone info exists.
    '''
    date_obj = parse_date(date_string)
    if date_obj.tzinfo is None:
        date_obj = date_obj.replace(tzinfo=pytz.UTC)
    return date_obj
