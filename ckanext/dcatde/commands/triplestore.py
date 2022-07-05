#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for the triple store.
'''
import sys
from ckan import model
from ckan.plugins.toolkit import CkanCommand
from ckan.plugins import toolkit as tk
from ckanext.dcatde.triplestore.fuseki_client import FusekiTriplestoreClient
from ckanext.dcatde.commands.command_util import reindex, clean_triplestore_from_uris
from ckanext.dcatde.validation.shacl_validation import ShaclValidator

RDF_FORMAT_TURTLE = 'turtle'


class Triplestore(CkanCommand):
    # pylint: disable=R0903
    '''Interacts with the triple store, e.g. reindex data.

    Usage:

      triplestore reindex [--dry-run]
        - Reindex all datasets edited manually in the GovData portal only and which are not imported
        automatically by a harvester.

      triplestore delete_datasets [--dry-run] [--uris]
        - Delete all datatsets from the ds-triplestore for the URIs given with the uris-option.

        '''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def __init__(self, name):
        super(Triplestore, self).__init__(name)
        self.parser.add_option('--dry-run', dest='dry_run', default='True',
                               help='With dry-run True the reindex will be not executed. '
                               'The default is True.')
        self.parser.add_option('--uris', dest='uris', default='',
                               help='Use comma separated URI-values to specify which datasets ' \
                                    'should be deleted when running delete_datasets')

        self.admin_user = None
        self.triplestore_client = None
        self.shacl_validation_client = None
        self.dry_run = True
        self.uris_to_clean = ''

    def command(self):
        '''Executes commands.'''
        super(Triplestore, self)._load_config()

        if len(self.args) != 1:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]

        # Getting/Setting default site user
        context = {'model': model, 'session': model.Session, 'ignore_auth': True}
        self.admin_user = tk.get_action('get_site_user')(context, {})

        if cmd == 'reindex':
            self._check_options()
            # Initialize triple store client
            self.triplestore_client = FusekiTriplestoreClient()
            self.shacl_validation_client = ShaclValidator()
            reindex(self.dry_run, self.triplestore_client, self.shacl_validation_client, self.admin_user)
        elif cmd == 'delete_datasets':
            self._check_options()
            self.triplestore_client = FusekiTriplestoreClient()
            clean_triplestore_from_uris(self.dry_run, self.triplestore_client, self.uris_to_clean)
        else:
            print('Command {0} not recognized'.format(cmd))

    def _check_options(self):
        '''Checks available options.'''
        if self.options.dry_run:
            if self.options.dry_run.lower() not in ('yes', 'true', 'no', 'false'):
                self.parser.error('Value \'%s\' for dry-run is not a boolean!' \
                                  % str(self.options.dry_run))
            elif self.options.dry_run.lower() in ('no', 'false'):
                self.dry_run = False
        if self.options.uris:
            self.uris_to_clean = self.options.uris.split(",")
