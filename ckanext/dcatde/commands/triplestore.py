#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
Paster command for the triple store.
'''
import sys
import time

from rdflib import URIRef
from SPARQLWrapper.SPARQLExceptions import SPARQLWrapperException
from ckan.lib.base import model
from ckan.lib.cli import CkanCommand
from ckan.plugins import toolkit as tk
from ckanext.dcat.processors import RDFParserException, RDFParser
from ckanext.dcatde.profiles import DCATDE
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
            self._reindex()
        elif cmd == 'delete_datasets':
            self._check_options()
            self.triplestore_client = FusekiTriplestoreClient()
            self._clean_triplestore_from_uris()
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
        if self.options.uris:
            self.uris_to_clean = self.options.uris.split(",")

    def _reindex(self):
        '''Deletes all datasets matching package search filter query.'''
        starttime = time.time()
        package_obj_to_reindex = gather_dataset_ids(include_private=False)
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

            contributor_id = self._get_contributor_id(uri, rdf_parser)
            # shacl-validate the graph
            validation_rdf = self.shacl_validation_client.validate(rdf, uri, package_org, contributor_id)
            if validation_rdf:
                # update in mqa-triplestore
                self.triplestore_client.delete_dataset_in_triplestore_mqa(uri)
                self.triplestore_client.create_dataset_in_triplestore_mqa(validation_rdf, uri)

        return uri

    @staticmethod
    def _get_contributor_id(uri, rdf_parser):
        '''Gets the first contributorID from the DCAT-AP.de list within the graph.'''
        for contributor_id in rdf_parser.g.objects(uri, URIRef(DCATDE.contributorID)):
            candidate = str(contributor_id)
            # A dataset should only have one contributorID from the DCAT-AP.de list. So, just pick the first
            # element.
            if candidate.startswith('http://dcat-ap.de/def/contributors/'):
                return candidate

        return None

    def _clean_triplestore_from_uris(self):
        '''Delete dataset-uris from args from the triplestore'''
        if self.uris_to_clean == '':
            print "INFO: Missing Arg 'uris'." \
                "Use comma separated URI-values to specify which datasets should be deleted."
            return
        if self.dry_run:
            print "INFO: DRY-RUN: Deleting datasets is disabled."

        if self.triplestore_client.is_available():
            starttime = time.time()
            for uri in self.uris_to_clean:
                print "Deleting dataset with URI: " + uri
                if not self.dry_run:
                    self.triplestore_client.delete_dataset_in_triplestore(uri)
            endtime = time.time()
            print "INFO: Total time: %s." % (str(endtime - starttime))
        else:
            print "INFO: TripleStore is not available. Skipping cleaning!"
