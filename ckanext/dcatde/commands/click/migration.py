'''
Ckan command for migrating CKAN datasets from OGD to DCAT-AP.de.
'''
import click
import sys
import json
import socket
import sys
import time

from ckan.plugins import toolkit as tk
from ckan.lib.base import model
from sqlalchemy import or_
from ckanext.dcatde import dataset_utils
from ckanext.dcatde.dataset_utils import gather_dataset_ids, set_extras_field, get_extras_field
from ckanext.dcatde.migration import migration_functions, util
from ckan.logic import UnknownValidator, schema as schema_

EXTRA_KEY_ADMS_IDENTIFIER = 'alternate_identifier'
EXTRA_KEY_DCT_IDENTIFIER = 'identifier'
EXTRA_KEY_CONTRIBUTOR_ID = 'contributorID'

DEPRECATED_CONTRIBUTOR_IDS = {
    # key: old value || value: new value
    'http://dcat-ap.de/def/contributors/bundesanstaltFuerLandwirtschaftUndErnaehrung':
    'http://dcat-ap.de/def/contributors/bundesministeriumFuerErnaehrungUndLandwirtschaft'}

UDP_IP = "127.0.0.1"
UDP_PORT = 5005

@click.command('dcatde_migrate')
@click.argument('args', nargs=-1)
def dcatde_migrate(args):
    '''
    Migrates CKAN datasets from OGD to DCAT-AP.de.

    Usage: dcatde_migrate [dry-run] [adms-id-migrate] [contributor-id-migrate]
    Params:
        dry-run             If given, perform all migration tasks without saving. A full
                            log file is written.

        adms-id-migrate     If given, only migrate adms:identifier to dct:identifier for all affected
                            datasets.

        contributor-id-migrate If given, set a contributor-ID for all datasets without an ID.

    Connect with "nc -ul 5005" on the same machine to receive status updates.
    '''
    return dcatde_migrate_command(args)

def package_schema():
    PACKAGE_UPDATE_SCHEMA = schema_.default_update_package_schema()
    try:
        email_validator = tk.get_validator('email_validator')
        PACKAGE_UPDATE_SCHEMA['maintainer_email'].remove(email_validator)
        PACKAGE_UPDATE_SCHEMA['author_email'].remove(email_validator)
    except (ValueError, UnknownValidator):
        pass
    return PACKAGE_UPDATE_SCHEMA
    
def dcatde_migrate_command(args):
    # constants for different migration modes
    MODE_OGD = 0
    MODE_ADMS_ID = 1
    MODE_CONTRIBUTOR_ID = 2

    dry_run = False
    migration_mode = MODE_OGD

    # Executes command.
    for cmd in args:
        if cmd == 'dry-run':
            dry_run = True
        elif cmd == 'adms-id-migrate':
            migration_mode = MODE_ADMS_ID
        elif cmd == 'contributor-id-migrate':
            migration_mode = MODE_CONTRIBUTOR_ID
        else:
            print('Command %s not recognized' % cmd)
            sys.exit(1)

    if migration_mode == MODE_ADMS_ID:
        migrate_adms_identifier(dry_run=dry_run)
    elif migration_mode == MODE_CONTRIBUTOR_ID:
        migrate_contributor_identifier(dry_run=dry_run)
    else:
        executor = migration_functions.MigrationFunctionExecutor(
            tk.config.get('ckanext.dcatde.urls.license_mapping'),
            tk.config.get('ckanext.dcatde.urls.category_mapping'))
        migrate_datasets(dry_run=dry_run, executor=executor)

def create_context(with_user=False):
    '''
    Creates new context.
    '''
    context = {'model': model, 'ignore_auth': True}
    if with_user:
        # Create context with user
        context['session'] = model.Session
        user = tk.get_action('get_site_user')(context, {})
        context['user'] = user['name']
        return context
    
    return context

def migrate_datasets(**kwargs):
    '''
    Iterates over all datasets and migrates fields with 'migration_functions'
    '''
    dry_run = kwargs['dry_run']
    executor = kwargs['executor']

    # Check if all needed groups are present
    group_list = tk.get_action('group_list')
    if not executor.check_group_presence(group_list(create_context(), {})):
        return

    util.get_migrator_log().info(
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

    for dataset in iterate_local_datasets():
        executor.apply_to(dataset)

        update_dataset(dataset, dry_run=dry_run)

    util.get_migrator_log().info(
        'Dataset migration finished' +
        (' [dry run, did not save]' if dry_run else ''))

def migrate_adms_identifier(**kwargs):
    '''
    Iterates over all datasets and migrates fields with 'adms_identifier'
    '''
    dry_run = kwargs['dry_run']

    util.get_migrator_log().info(
        'Migrating adms:identifier to dct:identifier' +
        (' [dry run without saving]' if dry_run else ''))

    for dataset in iterate_adms_id_datasets():
        # only migrate if dct:identifier is not already present
        if not dataset_utils.get_extras_field(dataset, EXTRA_KEY_DCT_IDENTIFIER):
            util.rename_extras_field_migration(dataset, EXTRA_KEY_ADMS_IDENTIFIER,
                                                EXTRA_KEY_DCT_IDENTIFIER, False)
            update_dataset(dataset, dry_run=dry_run)
        else:
            util.get_migrator_log().info(
                '%sSkipping package as it already has a dct:identifier',
                util.log_dataset_prefix(dataset)
            )

    util.get_migrator_log().info(
        'Finished migration of adms:identifier to dct:identifier' +
        (' [dry run without saving]' if dry_run else ''))

def migrate_contributor_identifier(**kwargs):
    ''' Add govdata-contributor-IDs to datasets that are missing one '''
    dry_run = kwargs['dry_run']

    util.get_migrator_log().info(
        'Migrating dcatde:contributorID' + (' [dry run without saving]' if dry_run else ''))

    starttime = time.time()
    package_obj_to_update = gather_dataset_ids()
    endtime = time.time()
    print("INFO: %s datasets found to check for contributor-ID. Total time: %s." % \
            (len(package_obj_to_update), str(endtime - starttime)))

    organization_list = tk.get_action('organization_list')(create_context(),
                                                            {'all_fields': True, 'include_extras': True})
    updated_count = created_count = deprecated_count = 0

    starttime = time.time()

    for dataset in iterate_datasets(package_obj_to_update.keys()):
        print(u'[DEBUG] Checking dataset: {}'.format(dataset['title']))

        dataset_org_id = dataset['organization']['id']
        dataset_org = next((item for item in organization_list if item['id'] == dataset_org_id), None)
        if not dataset_org:
            print(u'[INFO] Did not find a Organization for ID: ' + dataset_org_id)
            continue

        org_contributor_field = get_extras_field(dataset_org, EXTRA_KEY_CONTRIBUTOR_ID)
        if not org_contributor_field:
            print(u'[INFO] Did not find a contributor ID for Organization: ' + dataset_org_id)
            continue

        try:
            org_contributor_id_list = json.loads(org_contributor_field['value'])
        except ValueError:
            # json.loads failed -> value is not an array but a single string
            org_contributor_id_list = [org_contributor_field['value']]

        dataset_contributor_field = get_extras_field(dataset, EXTRA_KEY_CONTRIBUTOR_ID)
        requires_update = False
        if not dataset_contributor_field:
            # Contributor-id field does not exist yet
            set_extras_field(dataset, EXTRA_KEY_CONTRIBUTOR_ID, json.dumps(org_contributor_id_list))
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
                set_extras_field(dataset, EXTRA_KEY_CONTRIBUTOR_ID, json.dumps(current_ids_list_unique))

        if requires_update:
            update_dataset(dataset, dry_run=dry_run)

    endtime = time.time()
    print("INFO: %s deprecated Contributor-IDs were updated." % deprecated_count)
    print("INFO: A Contributor-ID was created for %s datasets that did not have one before." % \
            created_count)
    print("INFO: %s datasets were updated. Total time: %s." % (updated_count, str(endtime - starttime)))

    util.get_migrator_log().info(
        'Finished migration of dcatde:contributorID' +
        (' [dry run without saving]' if dry_run else ''))

def iterate_datasets(package_ids):
    '''
    Helper which iterates over all datasets in package_ids, i.e. fetches the package
    for all IDs
    '''
    package_show = tk.get_action('package_show')

    package_ids_unique = set(package_ids)
    progress_total = len(package_ids_unique)
    util.get_migrator_log().info('INFO migrating ' + str(progress_total) + ' datasets in total')
    progress_current = 0
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    for dataset_id in package_ids_unique:
        try:
            # write out status via UDP (see class doc for netcat cmd)
            progress_current += 1
            sock_bytes = str(progress_current) + " / " + str(progress_total) + "\n"
            sock.sendto(sock_bytes.encode("utf-8"), (UDP_IP, UDP_PORT))

            dataset = package_show(create_context(), {'id': dataset_id.strip()})

            # ignore harvesters, which are in the list as well
            if dataset['type'] == 'harvest':
                continue

            yield dataset

        except Exception:
            util.get_migrator_log().exception("Package '%s' was not found",
                                                dataset_id)

def iterate_local_datasets():
    '''
    Iterates over all local datasets
    '''
    package_list = tk.get_action('package_list')

    # returns only active datasets (missing datasets with status "private" and "draft")
    package_ids = package_list(create_context(), {})
    # Query all private and draft packages except harvest packages
    query = model.Session.query(model.Package)\
        .filter(or_(model.Package.private == True, model.Package.state == 'draft'))\
        .filter(model.Package.type != 'harvest')
    for package_object in query:
        package_ids.append(package_object.id)

    return iterate_datasets(package_ids)

def iterate_adms_id_datasets():
    '''
    Iterates over all datasets having an adms:identifier (extras.alternate_identifier) field
    '''
    query = model.Session.query(model.PackageExtra.package_id) \
        .filter(model.PackageExtra.key == EXTRA_KEY_ADMS_IDENTIFIER) \
        .filter(model.PackageExtra.state != 'deleted')
    package_ids = []
    for package_object in query:
        package_ids.append(package_object.package_id)

    return iterate_datasets(package_ids)

def update_dataset(dataset, **kwargs):
    '''
    Updates dataset in CKAN.
    '''
    dry_run = kwargs['dry_run']

    if not dry_run:
        try:
            package_update = tk.get_action('package_update')
            ctx = create_context(with_user=True)
            ctx['schema'] = package_schema()
            ctx['return_id_only'] = True
            package_update(ctx, dataset)
        except Exception:
            util.get_migrator_log().exception(
                util.log_dataset_prefix(dataset) + 'could not update')
