#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for the triple store.
'''
import sys
import time

from SPARQLWrapper.SPARQLExceptions import SPARQLWrapperException
from ckan.lib.base import model
from ckan.lib.cli import CkanCommand
from ckan.plugins import toolkit as tk
from ckanext.dcat.processors import RDFParserException, RDFParser
from ckanext.dcatde.triplestore.fuseki_client import FusekiTriplestoreClient
from ckanext.dcatde.validation.shacl_validation import ShaclValidator
from ckanext.dcatde.dataset_utils import gather_dataset_ids

RDF_FORMAT_TURTLE = 'turtle'


class Triplestore(CkanCommand):
    '''Interacts with the triple store, e.g. reindex data.

    Usage:

      triplestore reindex [--dry-run]
        - Reindex all datasets edited manually in the GovData portal only and which are not imported
        automatically by a harvester.

        '''

    summary = __doc__.split('\n')[0]
    usage = __doc__

    def __init__(self, name):
        super(Triplestore, self).__init__(name)
        self.parser.add_option('--dry-run', dest='dry_run', default='True',
                               help='With dry-run True the reindex will be not executed. '
                               'The default is True.')

        self.admin_user = None
        self.triplestore_client = None
        self.shacl_validation_client = None
        self.dry_run = True

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
            self._reindex()
        else:
            print u'Command {0} not recognized'.format(cmd)

    def _check_options(self):
        '''Checks available options.'''
        if self.options.dry_run:
            if self.options.dry_run.lower() not in ('yes', 'true', 'no', 'false'):
                self.parser.error('Value \'%s\' for dry-run is not a boolean!' \
                                  % str(self.options.dry_run))
            elif self.options.dry_run.lower() in ('no', 'false'):
                self.dry_run = False

    def _reindex(self):
        '''Deletes all datasets matching package search filter query.'''
        starttime = time.time()
        package_obj_to_reindex = gather_dataset_ids()
        endtime = time.time()
        print "INFO: %s datasets found to reindex. Total time: %s." % \
                (len(package_obj_to_reindex), str(endtime - starttime))

        if self.dry_run:
            print "INFO: DRY-RUN: The dataset reindex is disabled."
            print "DEBUG: Package IDs:"
            print package_obj_to_reindex.keys()
        elif package_obj_to_reindex:
            print 'INFO: Start updating triplestore...'
            success_count = error_count = 0
            starttime = time.time()
            if self.triplestore_client.is_available():
                for package_id, package_org in package_obj_to_reindex.iteritems():
                    try:
                        # Reindex package
                        checkpoint_start = time.time()
                        uri = self._update_package_in_triplestore(package_id, package_org)
                        checkpoint_end = time.time()
                        print "DEBUG: Reindexed dataset with id %s. Time taken for reindex: %s." % \
                                 (package_id, str(checkpoint_end - checkpoint_start))
                        success_count += 1
                    except RDFParserException as ex:
                        print u'ERROR: While parsing the RDF file: {0}'.format(ex)
                        error_count += 1
                    except SPARQLWrapperException as ex:
                        print u'ERROR: Unexpected error while updating dataset with URI %s: %s' % (uri, ex)
                        error_count += 1
                    except Exception as error:
                        print u'ERROR: While reindexing dataset with id %s. Details: %s' % \
                                (package_id, error.message)
                        error_count += 1
            else:
                print "INFO: TripleStore is not available. Skipping reindex!"
            endtime = time.time()
            print '============================================================='
            print "INFO: %s datasets successfully reindexed. %s datasets couldn't reindexed. "\
            "Total time: %s." % (success_count, error_count, str(endtime - starttime))

    def _get_rdf(self, dataset_ref):
        '''Reads the RDF presentation of the dataset with the given ID.'''
        context = {'user': self.admin_user['name']}
        return tk.get_action('dcat_dataset_show')(context, {'id': dataset_ref, 'format': RDF_FORMAT_TURTLE})

    def _update_package_in_triplestore(self, package_id, package_org):
        '''Updates the package with the given package ID in the triple store.'''
        uri = 'n/a'
        # Get uri of dataset
        rdf = self._get_rdf(package_id)
        rdf_parser = RDFParser()
        rdf_parser.parse(rdf, RDF_FORMAT_TURTLE)
        # Should be only one dataset
        for uri in rdf_parser._datasets():
            self.triplestore_client.delete_dataset_in_triplestore(uri)
            self.triplestore_client.create_dataset_in_triplestore(rdf, uri)

            # shacl-validate the graph
            validation_rdf = self.shacl_validation_client.validate(rdf, uri, package_org)
            if validation_rdf:
                # update in mqa-triplestore
                self.triplestore_client.delete_dataset_in_triplestore_mqa(uri, package_org)
                self.triplestore_client.create_dataset_in_triplestore_mqa(validation_rdf, uri)

        return uri
