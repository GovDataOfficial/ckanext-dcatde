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
import ckan.plugins.toolkit as tk
from rdflib import Graph
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import or_, and_, not_
from ckanext.dcat.processors import RDFParserException, RDFParser
from ckanext.dcatde.dataset_utils import EXTRA_KEY_HARVESTED_PORTAL
from ckanext.dcatde.triplestore.fuseki_client import FusekiTriplestoreClient
import ckanext.harvest.model as harvest_model


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
        package_ids_to_reindex = self._gather_dataset_ids()
        endtime = time.time()
        print "INFO: %s datasets found to reindex. Total time: %s." % \
                (len(package_ids_to_reindex), str(endtime - starttime))

        if self.dry_run:
            print "INFO: DRY-RUN: The dataset reindex is disabled."
            print "DEBUG: Package IDs:"
            print package_ids_to_reindex
        elif package_ids_to_reindex:
            print 'INFO: Start updating triplestore...'
            success_count = error_count = 0
            starttime = time.time()
            if self.triplestore_client.is_available():
                for package_id in package_ids_to_reindex:
                    try:
                        # Reindex package
                        checkpoint_start = time.time()
                        uri = self._update_package_in_triplestore(package_id)
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

    @staticmethod
    def _gather_dataset_ids():
        '''Collects all dataset ids to reindex.'''
        package_ids_found = []
        # pylint: disable=E1101
        # read orgs related to a harvest source
        subquery_harvest_orgs = model.Session.query(model.Group.id).distinct() \
            .join(model.Package, model.Package.owner_org == model.Group.id) \
            .join(harvest_model.HarvestSource, harvest_model.HarvestSource.id == model.Package.id)\
            .filter(model.Package.state == model.State.ACTIVE) \
            .filter(harvest_model.HarvestSource.active.is_(True)) \
            .filter(model.Group.state == model.State.ACTIVE) \
            .filter(model.Group.is_organization.is_(True)) \
            .subquery()

        # read all package IDs to reindex
        package_extra_alias = aliased(model.PackageExtra)

        query = model.Session.query(model.Package.id).distinct() \
            .outerjoin(model.PackageExtra, model.PackageExtra.package_id == model.Package.id)\
            .filter(model.Package.type != 'harvest')\
            .filter(model.Package.state == model.State.ACTIVE) \
            .filter(or_(model.Package.owner_org.notin_(subquery_harvest_orgs),
                        and_(model.Package.owner_org.in_(subquery_harvest_orgs),
                             not_(model.Session.query(model.Package.id)
                                  .filter(and_(model.Package.id == package_extra_alias.package_id,
                                               package_extra_alias.state == model.State.ACTIVE,
                                               package_extra_alias.key == EXTRA_KEY_HARVESTED_PORTAL))
                                  .exists()))))
        # pylint: enable=E1101

        for row in query:
            package_ids_found.append(row[0])

        return set(package_ids_found)

    def _get_rdf(self, dataset_ref):
        '''Reads the RDF presentation of the dataset with the given ID.'''
        context = {'user': self.admin_user['name']}
        return tk.get_action('dcat_dataset_show')(context, {'id': dataset_ref})

    def _update_package_in_triplestore(self, package_id):
        '''Updates the package with the given package ID in the triple store.'''
        uri = 'n/a'
        # Get uri of dataset
        rdf = self._get_rdf(package_id)
        rdf_parser = RDFParser()
        rdf_parser.parse(rdf)
        # Should be only one dataset
        for uri in rdf_parser._datasets():
            self.triplestore_client.delete_dataset_in_triplestore(uri)
            # Get rdf graph from rdf serialization
            graph = Graph()
            for triple in rdf_parser.g.triples((None, None, None)):
                graph.add(triple)

            rdf_graph = graph.serialize(format="xml")
            self.triplestore_client.create_dataset_in_triplestore(rdf_graph, uri)

        return uri
