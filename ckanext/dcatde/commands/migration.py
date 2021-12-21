#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for migrating CKAN datasets from OGD to DCAT-AP.de.
'''
from ckan.logic import UnknownValidator, schema as schema_
from ckan.plugins import toolkit as tk
from ckanext.dcatde.commands.cli import CkanCommand
from ckanext.dcatde.utils import migration

class DCATdeMigrateCommand(CkanCommand):
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

    summary = __doc__.split('\n')[0]
    usage = __doc__

    PACKAGE_UPDATE_SCHEMA = schema_.default_update_package_schema()

    def __init__(self, name):
        super(DCATdeMigrateCommand, self).__init__(name)
        self.executor = None  # initialized after config load
        try:
            email_validator = tk.get_validator('email_validator')
            self.PACKAGE_UPDATE_SCHEMA['maintainer_email'].remove(email_validator)
            self.PACKAGE_UPDATE_SCHEMA['author_email'].remove(email_validator)
        except (ValueError, UnknownValidator):
            pass

    def command(self):
        '''
        Executes command.
        '''
        self._load_config()
        return migration(self.args)
