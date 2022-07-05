#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Paster command for adding DCAT themes (categories) to the CKAN instance.
"""
import json
import sys

import ckanapi
from six.moves import urllib
import ckan.plugins.toolkit as tk
from ckan.plugins.toolkit import CkanCommand
from ckanext.dcatde.commands.command_util import create_groups, migrate_user_permissions


class ThemeAdder(CkanCommand):
    # pylint: disable=R0903
    """
    Adds a default set of groups to the current CKAN instance.

    Usage: dcatde_themeadder [omit-group-migration]
    """

    summary = __doc__.split('\n')[0]
    usage = __doc__

    omit_group_migration = False

    def __init__(self, name):
        super(ThemeAdder, self).__init__(name)
        self.admin_user = None

    def command(self):
        """Worker command doing the actual group additions."""

        if len(self.args) > 0:
            cmd = self.args[0]

            if cmd == 'omit-group-migration':
                self.omit_group_migration = True
            else:
                print('Command %s not recognized' % cmd)
                self.parser.print_usage()
                sys.exit(1)

        super(ThemeAdder, self)._load_config()
        ckan_api_client = ckanapi.LocalCKAN()

        present_groups_dict = ckan_api_client.action.group_list()

        present_groups_keys = []
        if len(present_groups_dict) > 0:
            for group_key in present_groups_dict:
                present_groups_keys.append(group_key)

        groups_file = tk.config.get('ckanext.dcatde.urls.themes')
        try:
            groups_str = urllib.request.urlopen(groups_file).read()
        except Exception:
            print('Could not load group config file!')
            groups_str = '{}'

        govdata_groups = json.loads(groups_str)
        create_groups(present_groups_keys,govdata_groups,self.admin_user)

        if not self.omit_group_migration:
            migrate_user_permissions(present_groups_keys, govdata_groups, self.admin_user)
