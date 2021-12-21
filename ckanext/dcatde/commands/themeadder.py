#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Paster command for adding DCAT themes (categories) to the CKAN instance.
"""
from ckanext.dcatde.commands.cli import CkanCommand
from ckanext.dcatde.utils import themeadder_command


class ThemeAdder(CkanCommand):
    """
    Adds a default set of groups to the current CKAN instance.

    Usage: dcatde_themeadder [omit-group-migration]
    Params:
        dry-run    If given, do not migrate group permissions to newly
                   created groups.
    """

    summary = __doc__.split('\n')[0]
    usage = __doc__

    omit_group_migration = False

    def __init__(self, name):
        super(ThemeAdder, self).__init__(name)

    def command(self):
        """Worker command doing the actual group additions."""

        super(ThemeAdder, self)._load_config()
        return themeadder_command(self.args)
