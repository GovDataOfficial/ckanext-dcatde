'''
Ckan command for the triple store.
'''
import click
import time
from ckanext.dcatde.dataset_utils import gather_dataset_ids
from ckanext.dcatde.triplestore.fuseki_client import FusekiTriplestoreClient
from ckanext.dcatde.validation.shacl_validation import ShaclValidator
from ckanext.dcatde.validation.shacl_validation import ShaclValidator
from SPARQLWrapper.SPARQLExceptions import SPARQLWrapperException
from ckanext.dcat.processors import RDFParserException, RDFParser
from ckan.plugins import toolkit as tk
from ckan.lib.base import model

RDF_FORMAT_TURTLE = 'turtle'
triplestore_client = FusekiTriplestoreClient()
shacl_validation_client = ShaclValidator()

context = {
    'model': model,
    'session': model.Session,
    'ignore_auth': True
}

@click.group()
def triplestore():
    """Interacts with the triple store, e.g. reindex data.

    Usage:

      triplestore reindex [--dry-run]
        - Reindex all datasets edited manually in the GovData portal only and which are not imported
        automatically by a harvester.

      triplestore delete_datasets [--dry-run] [--uris]
        - Delete all datatsets from the ds-triplestore for the URIs given with the uris-option.

    """
    pass

@triplestore.command()
@click.option('--dry-run', default=True, help='With dry-run True the reindex \
    will be not executed. The default is True.')
def reindex(dry_run):
    """
    Reindex all datasets edited manually in the GovData portal only and which \
    are not imported automatically by a harvester.
    """
    result = _check_options(dry_run=dry_run)
    return _reindex(result["dry_run"])

@triplestore.command()
@click.option('--dry-run', default=True, help='With dry-run True the reindex \
    will be not executed. The default is True.')
@click.option('--uris', default='', help='Use comma separated URI-values to \
    specify which datasets should be deleted when running delete_datasets')
def delete_datasets(dry_run, uris):
    """
    Reindex all datasets edited manually in the GovData portal only and which \
    are not imported automatically by a harvester.
    """
    result = _check_options(dry_run=dry_run, uris=uris)
    return _clean_triplestore_from_uris(result['dry_run'], result['uris'])

def _check_options(**kwargs):
    '''Checks available options.'''
    uris_to_clean = []
    dry_run = kwargs.get("dry_run", True)
    if dry_run:
        if str(dry_run).lower() not in ('yes', 'true', 'no', 'false'):
            click.Abort('Value \'%s\' for dry-run is not a boolean!' \
                                % str(dry_run))
        elif str(dry_run).lower() in ('no', 'false'):
            dry_run = False
    if kwargs.get("uris", None):
        uris_to_clean = str(kwargs["uris"]).split(",")

    return {
        "dry_run": dry_run,
        "uris": uris_to_clean
    }

def _get_rdf(dataset_ref, admin_user):
    '''Reads the RDF presentation of the dataset with the given ID.'''
    # Getting/Setting default site user
    context = {'user': admin_user['name']}
    return tk.get_action('dcat_dataset_show')(context, {
        'id': dataset_ref, 'format': RDF_FORMAT_TURTLE})

def _update_package_in_triplestore(package_id, package_org, admin_user):
    '''Updates the package with the given package ID in the triple store.'''
    uri = 'n/a'
    # Get uri of dataset
    rdf = _get_rdf(package_id, admin_user)
    rdf_parser = RDFParser()
    rdf_parser.parse(rdf, RDF_FORMAT_TURTLE)
    # Should be only one dataset
    for uri in rdf_parser._datasets():
        triplestore_client.delete_dataset_in_triplestore(uri)
        triplestore_client.create_dataset_in_triplestore(rdf, uri)

        # shacl-validate the graph
        validation_rdf = shacl_validation_client.validate(rdf, uri, package_org)
        if validation_rdf:
            # update in mqa-triplestore
            triplestore_client.delete_dataset_in_triplestore_mqa(uri, package_org)
            triplestore_client.create_dataset_in_triplestore_mqa(validation_rdf, uri)

    return uri

def _reindex(dry_run, admin_user=None):
    '''Deletes all datasets matching package search filter query.'''
    starttime = time.time()
    package_obj_to_reindex = gather_dataset_ids(include_private=False)
    endtime = time.time()

    print("INFO: %s datasets found to reindex. Total time: %s." % \
            (len(package_obj_to_reindex), str(endtime - starttime)))
    
    if not admin_user:
        admin_user = tk.get_action('get_site_user')(context, {})

    if dry_run:
        print("INFO: DRY-RUN: The dataset reindex is disabled.")
        print("DEBUG: Package IDs:")
        print(package_obj_to_reindex.keys())
    elif package_obj_to_reindex:
        print('INFO: Start updating triplestore...')
        success_count = error_count = 0
        starttime = time.time()
        if triplestore_client.is_available():
            for package_id, package_org in package_obj_to_reindex.items():
                try:
                    # Reindex package
                    checkpoint_start = time.time()
                    uri = _update_package_in_triplestore(package_id, package_org, admin_user)
                    checkpoint_end = time.time()
                    print("DEBUG: Reindexed dataset with id %s. Time taken for reindex: %s." % \
                                (package_id, str(checkpoint_end - checkpoint_start)))
                    success_count += 1
                except RDFParserException as ex:
                    print(u'ERROR: While parsing the RDF file: {0}'.format(ex))
                    error_count += 1
                except SPARQLWrapperException as ex:
                    print(u'ERROR: Unexpected error while updating dataset with URI %s: %s' % (uri, ex))
                    error_count += 1
                except Exception as error:
                    print(u'ERROR: While reindexing dataset with id %s. Details: %s' % \
                            (package_id, error.message))
                    error_count += 1
        else:
            print("INFO: TripleStore is not available. Skipping reindex!")
        endtime = time.time()
        print('=============================================================')
        print("INFO: %s datasets successfully reindexed. %s datasets couldn't reindexed. "\
        "Total time: %s." % (success_count, error_count, str(endtime - starttime)))

def _clean_triplestore_from_uris(dry_run, uris):
    '''Delete dataset-uris from args from the triplestore'''
    if uris == '':
        print("INFO: Missing Arg 'uris'." \
            "Use comma separated URI-values to specify which datasets should be deleted.")
        return
    if dry_run:
        print("INFO: DRY-RUN: Deleting datasets is disabled.")

    if triplestore_client.is_available():
        starttime = time.time()
        for uri in uris:
            print("Deleting dataset with URI: " + uri)
            if not dry_run:
                triplestore_client.delete_dataset_in_triplestore(uri)
        endtime = time.time()
        print("INFO: Total time: %s." % (str(endtime - starttime)))
    else:
        print("INFO: TripleStore is not available. Skipping cleaning!")
