#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for migrating CKAN datasets from OGD to DCAT-AP.de.
'''
import sys

from ckan import model
from ckan.plugins.toolkit import CkanCommand
from ckanext.dcatde.commands.command_util import (
    migrate_adms_identifier, migrate_contributor_identifier, migrate_datasets)


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

    # constants for different migration modes
    MODE_OGD = 0
    MODE_ADMS_ID = 1
    MODE_CONTRIBUTOR_ID = 2

    dry_run = False
    migration_mode = MODE_OGD

    def __init__(self, name):
        super(DCATdeMigrateCommand, self).__init__(name)

    def create_context(self):
        '''
        Creates new context.
        '''
        return {'model': model, 'ignore_auth': True}

    def command(self):
        '''
        Executes command.
        '''
        for cmd in self.args:
            if cmd == 'dry-run':
                self.dry_run = True
            elif cmd == 'adms-id-migrate':
                self.migration_mode = self.MODE_ADMS_ID
            elif cmd == 'contributor-id-migrate':
                self.migration_mode = self.MODE_CONTRIBUTOR_ID
            else:
                print('Command %s not recognized' % cmd)
                self.parser.print_usage()
                sys.exit(1)

        self._load_config()
        if self.migration_mode == self.MODE_ADMS_ID:
            migrate_adms_identifier(self.dry_run)
        elif self.migration_mode == self.MODE_CONTRIBUTOR_ID:
            migrate_contributor_identifier(self.dry_run)
        else:
            migrate_datasets(self.dry_run)
