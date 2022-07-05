#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Commands util methods
'''
import json
import socket
import time

import six
from rdflib import URIRef
from sqlalchemy import or_
from SPARQLWrapper.SPARQLExceptions import SPARQLWrapperException
from ckan import model
from ckan.logic import NotFound
from ckan.logic import UnknownValidator, schema as schema_
from ckan.plugins import toolkit as tk
from ckanext.dcat.processors import RDFParserException, RDFParser
from ckanext.dcatde import dataset_utils
from ckanext.dcatde.migration import migration_functions, util as migration_util
from ckanext.dcatde.profiles import DCATDE

EXTRA_KEY_ADMS_IDENTIFIER = 'alternate_identifier'
EXTRA_KEY_DCT_IDENTIFIER = 'identifier'
EXTRA_KEY_CONTRIBUTOR_ID = 'contributorID'

UDP_IP = "127.0.0.1"
UDP_PORT = 5005

RDF_FORMAT_TURTLE = 'turtle'

DEPRECATED_CONTRIBUTOR_IDS = {
    # key: old value || value: new value
    'http://dcat-ap.de/def/contributors/bundesministeriumDesInnernFuerBauUndHeimat':
    'http://dcat-ap.de/def/contributors/bundesministeriumDesInnernUndHeimat',
    'http://dcat-ap.de/def/contributors/bundesministeriumFuerWirtschaftUndEnergie':
    'http://dcat-ap.de/def/contributors/bundesministeriumFuerWirtschaftUndKlimaschutz',
    'http://dcat-ap.de/def/contributors/dieBundesbeauftragteFuerDenDatenschutzUndDieInformationsfreiheit':
    'http://dcat-ap.de/def/contributors/derBundesbeauftragteFuerDenDatenschutzUndDieInformationsfreiheit'}


#######################################
###         migration utils         ###
#######################################

def migrate_datasets(dry_run):
    '''
    Iterates over all datasets and migrates fields with 'migration_functions'
    '''
    executor = migration_functions.MigrationFunctionExecutor(
                tk.config.get('ckanext.dcatde.urls.license_mapping'),
                tk.config.get('ckanext.dcatde.urls.category_mapping'))
    # Check if all needed groups are present
    group_list = tk.get_action('group_list')
    if not executor.check_group_presence(group_list(_create_context(), {})):
        return

    migration_util.get_migrator_log().info(
        'Starting dataset migration' +
        (' [dry run without saving]' if dry_run else ''))

    # Change the type of all datasets to 'dataset' via DB query, as package_update() doesn't
    # allow to set the type
    if not dry_run:
        model.Session.query(model.Package)\
            .filter(or_((model.Package.type == "datensatz"),
                        (model.Package.type == "app"),
                        (model.Package.type == "dokument")))\
            .update({"type": u'dataset'})
        model.repo.commit()

    for dataset in _iterate_local_datasets():
        executor.apply_to(dataset)

        _update_dataset(dataset, dry_run)

    migration_util.get_migrator_log().info(
        'Dataset migration finished' +
        (' [dry run, did not save]' if dry_run else ''))


def migrate_adms_identifier(dry_run):
    '''
    Iterates over all datasets and migrates fields with 'adms_identifier'
    '''
    migration_util.get_migrator_log().info(
        'Migrating adms:identifier to dct:identifier' +
        (' [dry run without saving]' if dry_run else ''))

    for dataset in _iterate_adms_id_datasets():
        # only migrate if dct:identifier is not already present
        if not dataset_utils.get_extras_field(dataset, EXTRA_KEY_DCT_IDENTIFIER):
            migration_util.rename_extras_field_migration(dataset, EXTRA_KEY_ADMS_IDENTIFIER,
                                                EXTRA_KEY_DCT_IDENTIFIER, False)
            _update_dataset(dataset, dry_run)
        else:
            migration_util.get_migrator_log().info(
                '%sSkipping package as it already has a dct:identifier',
                migration_util.log_dataset_prefix(dataset)
            )

    migration_util.get_migrator_log().info(
        'Finished migration of adms:identifier to dct:identifier' +
        (' [dry run without saving]' if dry_run else ''))


def migrate_contributor_identifier(dry_run):
    ''' Add govdata-contributor-IDs to datasets that are missing one '''
    migration_util.get_migrator_log().info(
        'Migrating dcatde:contributorID' + (' [dry run without saving]' if dry_run else ''))

    starttime = time.time()
    package_obj_to_update = dataset_utils.gather_dataset_ids()
    endtime = time.time()
    print("INFO: %s datasets found to check for contributor-ID. Total time: %s." % \
            (len(package_obj_to_update), str(endtime - starttime)))

    organization_list = tk.get_action('organization_list')(_create_context(),
                                                            {'all_fields': True, 'include_extras': True})
    updated_count = created_count = deprecated_count = 0

    starttime = time.time()

    for dataset in _iterate_datasets(package_obj_to_update.keys()):
        print(u'[DEBUG] Checking dataset: {}'.format(dataset['title']))

        dataset_org_id = dataset['organization']['id']
        dataset_org = next((item for item in organization_list if item['id'] == dataset_org_id), None)
        if not dataset_org:
            print(u'[INFO] Did not find a Organization for ID: ' + dataset_org_id)
            continue

        org_contributor_field = dataset_utils.get_extras_field(dataset_org, EXTRA_KEY_CONTRIBUTOR_ID)
        if not org_contributor_field:
            print('[INFO] Did not find a contributor ID for Organization: ' + dataset_org_id)
            continue

        try:
            org_contributor_id_list = json.loads(org_contributor_field['value'])
        except ValueError:
            # json.loads failed -> value is not an array but a single string
            org_contributor_id_list = [org_contributor_field['value']]

        dataset_contributor_field = dataset_utils.get_extras_field(dataset, EXTRA_KEY_CONTRIBUTOR_ID)
        requires_update = False
        if not dataset_contributor_field:
            # Contributor-id field does not exist yet
            dataset_utils.set_extras_field(dataset, EXTRA_KEY_CONTRIBUTOR_ID,
                                           json.dumps(org_contributor_id_list))
            created_count = created_count + 1
            requires_update = True
        else:
            try:
                current_ids_list = json.loads(dataset_contributor_field['value'])
            except ValueError:
                # json.loads failed -> value is not an array but a single string
                current_ids_list = [dataset_contributor_field['value']]

            for index, cur_id in enumerate(current_ids_list):
                # check for deprecated values
                if cur_id in DEPRECATED_CONTRIBUTOR_IDS:
                    print(u'[DEBUG] Found deprecated contributorID: %s. Replace with new value.' % cur_id)
                    current_ids_list[index] = DEPRECATED_CONTRIBUTOR_IDS[cur_id]
                    deprecated_count = deprecated_count + 1
                    requires_update = True

            for contributor_id in org_contributor_id_list:
                if contributor_id not in current_ids_list:
                    current_ids_list.append(contributor_id)
                    requires_update = True

            if requires_update:
                updated_count = updated_count + 1
                # Remove duplicate values in list
                current_ids_list_unique = list(set(current_ids_list))
                dataset_utils.set_extras_field(dataset, EXTRA_KEY_CONTRIBUTOR_ID,
                                               json.dumps(current_ids_list_unique))

        if requires_update:
            _update_dataset(dataset, dry_run)

    endtime = time.time()
    print("INFO: %s deprecated Contributor-IDs were updated." % deprecated_count)
    print("INFO: A Contributor-ID was created for %s datasets that did not have one before." % \
            created_count)
    print("INFO: %s datasets were updated. Total time: %s." % (updated_count, str(endtime - starttime)))

    migration_util.get_migrator_log().info(
        'Finished migration of dcatde:contributorID' +
        (' [dry run without saving]' if dry_run else ''))


def _iterate_datasets(package_ids):
    '''
    Helper which iterates over all datasets in package_ids, i.e. fetches the package
    for all IDs
    '''
    package_show = tk.get_action('package_show')

    package_ids_unique = set(package_ids)
    progress_total = len(package_ids_unique)
    migration_util.get_migrator_log().info('INFO migrating ' + str(progress_total) + ' datasets in total')
    progress_current = 0
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    for dataset_id in package_ids_unique:
        try:
            # write out status via UDP (see class doc for netcat cmd)
            progress_current += 1
            message = str(progress_current) + " / " + str(progress_total) + "\n"
            sock.sendto(message.encode(), (UDP_IP, UDP_PORT))

            dataset = package_show(_create_context(), {'id': dataset_id.strip()})

            # ignore harvesters, which are in the list as well
            if dataset['type'] == 'harvest':
                continue

            yield dataset

        except Exception:
            migration_util.get_migrator_log().exception("Package '%s' was not found",
                                                dataset_id)


def _iterate_local_datasets():
    '''
    Iterates over all local datasets
    '''
    package_list = tk.get_action('package_list')

    # returns only active datasets (missing datasets with status "private" and "draft")
    package_ids = package_list(_create_context(), {})
    # Query all private and draft packages except harvest packages
    query = model.Session.query(model.Package)\
        .filter(or_(model.Package.private.is_(True), model.Package.state == 'draft'))\
        .filter(model.Package.type != 'harvest')
    for package_object in query:
        package_ids.append(package_object.id)

    return _iterate_datasets(package_ids)

def _iterate_adms_id_datasets():
    '''
    Iterates over all datasets having an adms:identifier (extras.alternate_identifier) field
    '''
    query = model.Session.query(model.PackageExtra.package_id) \
        .filter(model.PackageExtra.key == EXTRA_KEY_ADMS_IDENTIFIER) \
        .filter(model.PackageExtra.state != 'deleted')
    package_ids = []
    for package_object in query:
        package_ids.append(package_object.package_id)

    return _iterate_datasets(package_ids)


def _update_dataset(dataset, dry_run):
    '''
    Updates dataset in CKAN.
    '''
    if not dry_run:
        try:
            package_update = tk.get_action('package_update')
            ctx = _create_context()
            ctx['schema'] = _get_update_package_schema()
            ctx['return_id_only'] = True
            package_update(ctx, dataset)
        except Exception:
            migration_util.get_migrator_log().exception(
                migration_util.log_dataset_prefix(dataset) + 'could not update')


def _create_context():
    '''
    Creates new context.
    '''
    return {'model': model, 'ignore_auth': True}


def _get_update_package_schema():
    '''
    Read and return update package schema
    '''
    schema = schema_.default_update_package_schema()  # pylint: disable=no-value-for-parameter
    try:
        email_validator = tk.get_validator('email_validator')
        schema['maintainer_email'].remove(email_validator)
        schema['author_email'].remove(email_validator)
    except (ValueError, UnknownValidator):
        # Support CKAN prior 2.7
        pass
    return schema


#######################################
###         themeadder utils        ###
#######################################

def create_groups(old_groups, new_groups, admin_user):
    for group_key in new_groups:
        if group_key not in old_groups:
            add_message = 'Adding group {group_key}.'.format(
                group_key=group_key
            )
            print(add_message)

            group_dict = {
                'name': group_key,
                'id': group_key,
                'title': new_groups[group_key]
            }

            _create_and_purge_group(
                group_dict, admin_user
            )
        else:
            skip_message = 'Skipping creation of group '
            skip_message = skip_message + "{group_key}, as it's already present."
            print((skip_message.format(group_key=group_key)))

def _create_and_purge_group(group_dict, admin_user):
    '''
    Worker method for the actual group addition.
    For unpurged groups a purge happens prior.
    '''

    try:
        tk.get_action('group_purge')(_create_context_with_user(admin_user), group_dict)
    except NotFound:
        not_found_message = 'Group {group_name} not found, nothing to purge.'.format(
            group_name=group_dict['name']
        )
        print(not_found_message)
    finally:
        tk.get_action('group_create')(_create_context_with_user(admin_user), group_dict)


def migrate_user_permissions(old_groups, new_groups, admin_user):
    '''
    Collects all users and their highest permission from old groups
    and sets them to new new groups.
    This is not marked private so it can be tested correctly.
    '''

    # roles a user can have, ordered by rank
    userrights = ["member", "editor", "admin"]

    # crawl existing groups and fetch users with permission
    groupdetails = tk.get_action('group_list')(_create_context_with_user(admin_user), {
        "include_users": True,
        "all_fields": True
    })

    users = {}
    for detail in groupdetails:
        if detail["name"] in old_groups:
            for user in detail["users"]:
                # store the highest ranking role
                if user["id"] in users and userrights.index(user["capacity"]) < userrights.index(
                        users[user["id"]]["capacity"]):
                    user["capacity"] = users[user["id"]]["capacity"]
                users[user["id"]] = user

    # add all users to new groups
    for user_id in users:
        for group in new_groups:
            username = users[user_id]["name"]
            role = users[user_id]["capacity"]
            print('Adding user {user} to group {group} having role {role}'.format(
                user=username,
                group=group,
                role=role))
            tk.get_action('group_member_create')(_create_context_with_user(admin_user), {
                "id": group,
                "username": username,
                "role": role
            })


def _create_context_with_user(admin_user):
    if not admin_user:
        # Getting/Setting default site user
        context = {'model': model, 'session': model.Session, 'ignore_auth': True}
        admin_user = tk.get_action('get_site_user')(context, {})

    return {'user': admin_user['name']}


#######################################
###        triplestore utils        ###
#######################################

def reindex(dry_run, triplestore_client, shacl_validation_client, admin_user):
    '''Deletes all datasets matching package search filter query.'''
    starttime = time.time()
    package_obj_to_reindex = dataset_utils.gather_dataset_ids(include_private=False)
    endtime = time.time()
    print("INFO: %s datasets found to reindex. Total time: %s." % \
            (len(package_obj_to_reindex), str(endtime - starttime)))

    if dry_run:
        print("INFO: DRY-RUN: The dataset reindex is disabled.")
        print("DEBUG: Package IDs:")
        print(list(package_obj_to_reindex.keys()))
    elif package_obj_to_reindex:
        print('INFO: Start updating triplestore...')
        success_count = error_count = 0
        starttime = time.time()
        if triplestore_client.is_available():
            for package_id, package_org in six.iteritems(package_obj_to_reindex):
                uri = 'n/a'
                try:
                    # Reindex package
                    checkpoint_start = time.time()
                    uri = _update_package_in_triplestore(triplestore_client, shacl_validation_client,
                                                         package_id, package_org, admin_user)
                    checkpoint_end = time.time()
                    print("DEBUG: Reindexed dataset with id %s. Time taken for reindex: %s." % \
                                (package_id, str(checkpoint_end - checkpoint_start)))
                    success_count += 1
                except RDFParserException as ex:
                    print('ERROR: While parsing the RDF file: {0}'.format(ex))
                    error_count += 1
                except SPARQLWrapperException as ex:
                    print('ERROR: Unexpected error while updating dataset with URI %s: %s' % (uri, ex))
                    error_count += 1
                except Exception as error:
                    print('ERROR: While reindexing dataset with id %s. Details: %s' % (package_id, error))
                    error_count += 1
        else:
            print("INFO: TripleStore is not available. Skipping reindex!")
        endtime = time.time()
        print('=============================================================')
        print("INFO: %s datasets successfully reindexed. %s datasets couldn't reindexed. "\
        "Total time: %s." % (success_count, error_count, str(endtime - starttime)))


def clean_triplestore_from_uris(dry_run, triplestore_client, uris_to_clean):
    '''Delete dataset-uris from args from the triplestore'''
    if uris_to_clean == '':
        print("INFO: Missing Arg 'uris'." \
            "Use comma separated URI-values to specify which datasets should be deleted.")
        return
    if dry_run:
        print("INFO: DRY-RUN: Deleting datasets is disabled.")

    if triplestore_client.is_available():
        starttime = time.time()
        for uri in uris_to_clean:
            print("Deleting dataset with URI: " + uri)
            if not dry_run:
                triplestore_client.delete_dataset_in_triplestore(uri)
        endtime = time.time()
        print("INFO: Total time: %s." % (str(endtime - starttime)))
    else:
        print("INFO: TripleStore is not available. Skipping cleaning!")


def _get_rdf(dataset_ref, admin_user):
    '''Reads the RDF presentation of the dataset with the given ID.'''
    context = {'user': admin_user['name']}
    return tk.get_action('dcat_dataset_show')(context, {'id': dataset_ref, 'format': RDF_FORMAT_TURTLE})


def _update_package_in_triplestore(triplestore_client, shacl_validation_client, package_id,
                                   package_org, admin_user):
    '''Updates the package with the given package ID in the triple store.'''
    uri = 'n/a'
    # Get uri of dataset
    rdf = _get_rdf(package_id, admin_user)
    rdf_parser = RDFParser()
    rdf_parser.parse(rdf, RDF_FORMAT_TURTLE)
    # Should be only one dataset
    for uri in rdf_parser._datasets():
        triplestore_client.delete_dataset_in_triplestore(uri)
        triplestore_client.create_dataset_in_triplestore(rdf, uri)

        contributor_id = _get_contributor_id(uri, rdf_parser)
        # shacl-validate the graph
        validation_rdf = shacl_validation_client.validate(rdf, uri, package_org, contributor_id)
        if validation_rdf:
            # update in mqa-triplestore
            triplestore_client.delete_dataset_in_triplestore_mqa(uri)
            triplestore_client.create_dataset_in_triplestore_mqa(validation_rdf, uri)

    return uri


def _get_contributor_id(uri, rdf_parser):
    '''Gets the first contributorID from the DCAT-AP.de list within the graph.'''
    for contributor_id in rdf_parser.g.objects(uri, URIRef(DCATDE.contributorID)):
        candidate = str(contributor_id)
        # A dataset should only have one contributorID from the DCAT-AP.de list. So, just pick the first
        # element.
        if candidate.startswith('http://dcat-ap.de/def/contributors/'):
            return candidate

    return None
