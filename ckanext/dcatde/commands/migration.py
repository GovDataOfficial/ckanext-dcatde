#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for migrating CKAN datasets from OGD to DCAT-AP.de.
'''
import socket
import sys

from ckan.lib.base import model
import ckan.plugins.toolkit as tk
from ckanext.dcatde.migration import migration_functions, util
import pylons
from sqlalchemy import or_


class DCATdeMigrateCommand(tk.CkanCommand):
    '''
    Migrates CKAN datasets from OGD to DCAT-AP.de.

    Usage: dcatde_migrate [dry-run]
    Params:
        dry-run    If given, perform all migration tasks without saving. A full
                   log file is written.

    Connect with "nc -ul 5005" on the same machine to receive status updates.
    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    UDP_IP = "127.0.0.1"
    UDP_PORT = 5005

    dry_run = False

    def __init__(self, name):
        super(DCATdeMigrateCommand, self).__init__(name)
        self.executor = None  # initialized after config load

    def create_context(self):
        '''
        Creates new context.
        '''
        return {'model': model, 'ignore_auth': True}

    def command(self):
        '''
        Executes command.
        '''
        if len(self.args) > 0:
            cmd = self.args[0]

            if cmd == 'dry-run':
                self.dry_run = True
            else:
                print 'Command %s not recognized' % cmd
                self.parser.print_usage()
                sys.exit(1)

        self._load_config()
        self.executor = migration_functions.MigrationFunctionExecutor(
            pylons.config.get('ckanext.dcatde.urls.license_mapping'),
            pylons.config.get('ckanext.dcatde.urls.category_mapping'))
        self.migrate_datasets()

    def migrate_datasets(self):
        '''
        Iterates over all datasets and migrates fields with 'migration_functions'
        '''
        # Check if all needed groups are present
        group_list = tk.get_action('group_list')
        if not self.executor.check_group_presence(group_list(self.create_context(), {})):
            return

        util.get_migrator_log().info(
            'Starting dataset migration' +
            (' [dry run without saving]' if self.dry_run else ''))

        # Change the type of all datasets to 'dataset' via DB query, as package_update() doesn't
        # allow to set the type
        if not self.dry_run:
            model.Session.query(model.Package)\
               .filter(or_((model.Package.type == "datensatz"),
                           (model.Package.type == "app"),
                           (model.Package.type == "dokument")))\
               .update({"type": u'dataset'})
            model.repo.commit()

        for dataset in self.iterate_local_datasets():
            self.executor.apply_to(dataset)

            self.update_dataset(dataset)

        util.get_migrator_log().info(
            'Dataset migration finished' +
            (' [dry run, did not save]' if self.dry_run else ''))

    def iterate_local_datasets(self):
        '''
        Iterates over all local datasets
        '''
        package_list = tk.get_action('package_list')
        package_show = tk.get_action('package_show')

        # returns only active datasets (missing datasets with status "private" and "draft")
        package_ids = package_list(self.create_context(), {})
        # Query all private and draft packages except harvest packages
        query = model.Session.query(model.Package)\
            .filter(or_(model.Package.private == True, model.Package.state == 'draft'))\
            .filter(model.Package.type != 'harvest')
        for package_object in query:
            package_ids.append(package_object.id)

        package_ids_unique = set(package_ids)
        progress_total = len(package_ids_unique)
        util.get_migrator_log().info('INFO migrating ' + str(progress_total) + ' datasets in total')
        progress_current = 0
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        for dataset_id in package_ids_unique:
            try:
                # write out status via UDP (see class doc for netcat cmd)
                progress_current += 1
                sock.sendto(str(progress_current) + " / " + str(progress_total) + "\n",
                            (self.UDP_IP, self.UDP_PORT))

                dataset = package_show(self.create_context(), {'id': dataset_id.strip()})

                # ignore harvesters, which are in the list as well
                if dataset['type'] == 'harvest':
                    continue

                yield dataset

            except Exception:
                util.get_migrator_log().exception("Package '%s' was not found",
                                                  dataset_id)

    def update_dataset(self, dataset):
        '''
        Updates dataset in CKAN
        '''
        if not self.dry_run:
            try:
                package_update = tk.get_action('package_update')
                ctx = self.create_context()
                ctx['return_id_only'] = True
                package_update(ctx, dataset)
            except Exception:
                util.get_migrator_log().exception(
                    util.log_dataset_prefix(dataset) + 'could not update')
