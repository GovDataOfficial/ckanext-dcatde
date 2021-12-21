"""
Ckan command for adding DCAT themes (categories) to the CKAN instance.
"""
import click
import json
import sys
import urllib.request as urllib2

from ckan import model
import ckan.plugins.toolkit as tk
import ckanapi
from ckan.logic import NotFound

@click.command("dcatde_themeadder")
@click.argument('args', nargs=-1)
def dcatde_themeadder(args):
    """
    Adds a default set of groups to the current CKAN instance.

    Usage: dcatde_themeadder [omit-group-migration]
    Params:
        dry-run    If given, do not migrate group permissions to newly
                   created groups.
    """
    return dcatde_themeadder_command(args)

def dcatde_themeadder_command(args):
    """Worker command doing the actual group additions."""

    omit_group_migration = False

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
        groups_str = urllib2.urlopen(groups_file).read()
    except Exception as e:
        print(e)
        print('Could not load group config file!')
        groups_str = '{}'

    govdata_groups = json.loads(groups_str)

    for group_key in govdata_groups:
        if group_key not in present_groups_keys:
            add_message = 'Adding group {group_key}.'.format(
                group_key=group_key
            )
            print(add_message)

            group_dict = {
                'name': group_key,
                'id': group_key,
                'title': govdata_groups[group_key]
            }

            _create_and_purge_group(
                group_dict
            )
        else:
            skip_message = 'Skipping creation of group '
            skip_message = skip_message + "{group_key}, as it's already present."
            print(skip_message.format(group_key=group_key))

    if not omit_group_migration:
        migrate_user_permissions(present_groups_keys, govdata_groups)

def create_context_with_user(admin_user=None):
        if not admin_user:
            # Getting/Setting default site user
            context = {'model': model, 'session': model.Session, 'ignore_auth': True}
            admin_user = tk.get_action('get_site_user')(context, {})

        return {'user': admin_user['name']}

def _create_and_purge_group(group_dict):
    """Worker method for the actual group addition.
    For unpurged groups a purge happens prior."""

    try:
        tk.get_action('group_purge')(create_context_with_user(), group_dict)
    except NotFound:
        not_found_message = 'Group {group_name} not found, nothing to purge.'.format(
            group_name=group_dict['name']
        )
        print(not_found_message)
    finally:
        tk.get_action('group_create')(create_context_with_user(), group_dict)

def migrate_user_permissions(old_groups, new_groups):
    """
    Collects all users and their highest permission from old groups
    and sets them to new new groups.
    This is not marked private so it can be tested correctly.
    """

    # roles a user can have, ordered by rank
    userrights = ["member", "editor", "admin"]

    # crawl existing groups and fetch users with permission
    groupdetails = tk.get_action('group_list')(create_context_with_user(), {
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
            tk.get_action('group_member_create')(create_context_with_user(), {
                "id": group,
                "username": username,
                "role": role
            })
