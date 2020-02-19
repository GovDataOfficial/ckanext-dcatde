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
        """

        renamed_package_ids = []
        package_update = p.toolkit.get_action('package_update')
        for package_dict in deprecated_package_dicts:
            context = HarvestUtils.build_context()
            package_id = package_dict['id']
            package_dict['name'] = HarvestUtils.create_new_name_for_deletion(package_dict['name'])
            # Update package
            try:
                package_update(context, package_dict)
                renamed_package_ids.append(package_id)
            except Exception as exception:
                LOGGER.error("Unable updating package %s: %s", package_id, exception)

        return renamed_package_ids

    @staticmethod
    def delete_packages(package_ids):
        """
        Deletes the packages belonging to the given package ids.
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
                    "Unable to delete package with id %s: %s",
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
        harvester_package = p.toolkit.get_action('package_show')(context, {'id': package_id})
        # rename and delete the package (renamed_ids contains the current package ID at most)
        renamed_ids = HarvestUtils.rename_datasets_before_delete([harvester_package])
        HarvestUtils.delete_packages(renamed_ids)

    @staticmethod
    def compare_metadata_modified(remote_modified, local_modified):
        '''
        Compares the modified datetimes of the metadata. Returns True if the remote date (first parameter)
        is newer.
        '''
        remote_dt = _parse_date(remote_modified)
        local_dt = _parse_date(local_modified)
        if remote_dt < local_dt:
            LOGGER.debug('remote dataset precedes local dataset -> skipping.')
            return False
        elif remote_dt == local_dt:
            LOGGER.debug('remote dataset equals local dataset -> skipping.')
            return False

        LOGGER.debug('local dataset precedes remote dataset -> importing.')
        # TODO do I have to delete other dataset?
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
            if orig_id:
                try:
                    data_dict = {"q": EXTRAS_KEY_DCT_IDENTIFIER + ':"' + orig_id + '"'}
                    # Add filter that local dataset guid is not equal to guid of the remote dataset
                    if remote_dataset_extras.key('guid'):
                        data_dict['fq'] = '-guid:"' + remote_dataset_extras.value('guid') + '"'
                    local_search_result = p.toolkit.get_action("package_search")(context, data_dict)
                    if local_search_result['count'] == 0:
                        LOGGER.debug('%sDid not find any existing dataset in the database. ' \
                            'Import accepted for %s.', method_prefix, remote_dataset_name)
                        return True
                    elif local_search_result['count'] == 1:
                        LOGGER.debug('%sFound duplicate entry for dataset %s.', method_prefix,
                                     remote_dataset_name)
                        local_dataset = local_search_result['results'][0]
                        local_dataset_extras = Extras(local_dataset['extras'])

                        # dct:modified vergleichen
                        if remote_dataset_extras.key(EXTRAS_KEY_DCT_MODIFIED) and \
                                local_dataset_extras.key(EXTRAS_KEY_DCT_MODIFIED):
                            return HarvestUtils.compare_metadata_modified(
                                remote_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED),
                                local_dataset_extras.value(EXTRAS_KEY_DCT_MODIFIED)
                            )
                        elif remote_dataset_extras.key('metadata_harvested_portal') == \
                                local_dataset_extras.key('metadata_harvested_portal'):
                            # Deletion will be done by the harvester, because the datasets are from the same
                            # metadata_harvested_portal (harvester)
                            LOGGER.debug(
                                '%sFound duplicate entry with the value "%s" in field "identifier", but ' \
                                'remote and/or local dataset does not contain a modified date. ' \
                                'No comparison possible! But the datasets are from the same harvester. ' \
                                'Import accepted for %s.',
                                method_prefix, orig_id, remote_dataset_name)
                            return True
                        # Kein Vergleich möglich, wenn nicht beide Datensätze ein modified date enthalten
                        else:
                            LOGGER.info(
                                '%sFound duplicate entry with the value "%s" in field "identifier", but ' \
                                'remote and/or local dataset does not contain a modified date. ' \
                                '-> Skipping import for %s!',
                                method_prefix, orig_id, remote_dataset_name)
                    else:
                        LOGGER.info('%sFound multiple duplicates with the value "%s" in field ' \
                            '"identifier". -> Skipping import for %s!', method_prefix, orig_id,
                                    remote_dataset_name)
                except Exception as exception:
                    LOGGER.error(exception)
            else:
                LOGGER.debug('%sNo original id in field identifier found. Import accepted for %s.',
                             method_prefix, remote_dataset_name)
                return True
        else:
            LOGGER.debug('%sNo field identifier found. Import accepted for %s.',
                         method_prefix, remote_dataset_name)
            return True

        return False


def _parse_date(date_string):
    '''
    Parse a date string to a date object and add the time zone UTC if no time zone info exists.
    '''
    date_obj = parse_date(date_string)
    if date_obj.tzinfo is None:
        date_obj = date_obj.replace(tzinfo=pytz.UTC)
    return date_obj
