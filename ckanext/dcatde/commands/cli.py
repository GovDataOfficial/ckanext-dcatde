# -*- coding: utf-8 -*-

import json
import sys

from six.moves import urllib

import ckanapi
import click
from ckan import model
from ckan.plugins import toolkit as tk
import ckanext.dcatde.commands.command_util as utils
from ckanext.dcatde.triplestore.fuseki_client import FusekiTriplestoreClient
from ckanext.dcatde.validation.shacl_validation import ShaclValidator

triplestore_client = FusekiTriplestoreClient()
shacl_validation_client = ShaclValidator()


def get_commands():
    ''' Get available commands '''
    return [dcatde_migrate,
            dcatde_themeadder,
            triplestore]


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
        utils.migrate_adms_identifier(dry_run=dry_run)
    elif migration_mode == MODE_CONTRIBUTOR_ID:
        utils.migrate_contributor_identifier(dry_run=dry_run)
    else:
        utils.migrate_datasets(dry_run=dry_run)


@click.command("dcatde_themeadder")
@click.argument('args', nargs=-1)
def dcatde_themeadder(args):
    '''
    Adds a default set of groups to the current CKAN instance.

    Usage: dcatde_themeadder [omit-group-migration]
    '''
    omit_group_migration = False
    admin_user = None

    if len(args) > 0:
        cmd = args[0]

        if cmd == 'omit-group-migration':
            omit_group_migration = True
        else:
            print('Command %s not recognized' % cmd)
            sys.exit(1)

    ckan_api_client = ckanapi.LocalCKAN()

    present_groups_dict = ckan_api_client.action.group_list()

    present_groups_keys = []
    if len(present_groups_dict) > 0:
        for group_key in present_groups_dict:
            present_groups_keys.append(group_key)

    groups_file = tk.config.get('ckanext.dcatde.urls.themes')

    try:
        groups_str = urllib.request.urlopen(groups_file).read()
    except Exception as e:
        print(e)
        print('Could not load group config file!')
        groups_str = '{}'

    govdata_groups = json.loads(groups_str)

    utils.create_groups(present_groups_keys, govdata_groups, admin_user)

    if not omit_group_migration:
        utils.migrate_user_permissions(present_groups_keys, govdata_groups, admin_user)


@click.group()
def triplestore():
    '''
    Interacts with the triple store, e.g. reindex data.

    Usage:

      triplestore reindex [--dry-run]
        - Reindex all datasets edited manually in the GovData portal only and which are not imported
        automatically by a harvester.

      triplestore delete_datasets [--dry-run] [--uris]
        - Delete all datatsets from the ds-triplestore for the URIs given with the uris-option.
    '''
    pass


@triplestore.command('reindex')
@click.option('--dry-run', default=True, help='With dry-run True the reindex \
    will be not executed. The default is True.', required=False)
def reindex(dry_run):
    """
    Reindex all datasets edited manually in the GovData portal only and which \
    are not imported automatically by a harvester.
    """
    result = _check_options(dry_run=dry_run)

    # Getting/Setting default site user
    context = {'model': model, 'session': model.Session, 'ignore_auth': True}
    admin_user = tk.get_action('get_site_user')(context, {})

    utils.reindex(result["dry_run"], triplestore_client, shacl_validation_client, admin_user)


@triplestore.command('delete_datasets')
@click.option('--dry-run', default=True, help='With dry-run True the reindex \
    will be not executed. The default is True.', required=False)
@click.option('--uris', default='', help='Use comma separated URI-values to \
    specify which datasets should be deleted when running delete_datasets')
def delete_datasets(dry_run, uris):
    """
    Delete all datasets for the given uris.
    """
    result = _check_options(dry_run=dry_run, uris=uris)
    utils.clean_triplestore_from_uris(result['dry_run'], triplestore_client, result['uris'])


def _check_options(**kwargs):
    '''Checks available options.'''
    uris_to_clean = []
    dry_run = kwargs.get("dry_run", True)
    if dry_run:
        if str(dry_run).lower() not in ('yes', 'true', 'no', 'false'):
            click.Abort('Value \'%s\' for dry-run is not a boolean!' \
                        % str(dry_run))
        elif str(dry_run).lower() in ('no', 'false'):
            dry_run = False
    if kwargs.get("uris", None):
        uris_to_clean = str(kwargs["uris"]).split(",")

    return {
        "dry_run": dry_run,
        "uris": uris_to_clean
    }
