# ckanext-dcatde

DCAT-AP.de specific CKAN extension for providing and importing DCAT-AP.de-Profile data.

## Dependencies

The CKAN-Plugin ckanext-dcatde is based on the CKAN extension [ckanext-dcat](https://github.com/ckan/ckanext-dcat).

For the RDF harvester, [ckanext-harvest](https://github.com/ckan/ckanext-harvest) is used (optional, see below).

## Getting Started

1. If you are using Python virtual environment (virtualenv), activate it.

2. Install a specific version of the CKAN extension ckanext-dcat. The ckanext-dcatde requires a release greater than `v1.0.0` of ckanext-dcat.

3. Install the extension on your virtualenv:

       # use project on GitHub
       (pyenv) $ pip install -e git+git://github.com/GovDataOfficial/ckanext-dcatde.git#egg=ckanext-dcatde
       (pyenv) $ cd src/ckanext-dcatde
       (pyenv) $ pip install -r base-requirements.txt -f requirements
       (pyenv) $ python setup.py develop

   or

       # use project on Open CoDE
       (pyenv) $ pip install -e git+git://gitlab.opencode.de/fitko/govdata/ckanext-dcatde.git#egg=ckanext-dcatde
       (pyenv) $ cd src/ckanext-dcatde
       (pyenv) $ pip install -r base-requirements.txt -f requirements
       (pyenv) $ python setup.py develop

4. [Install](https://github.com/ckan/ckanext-harvest#installation) a specific version of the CKAN extension ckanext-harvest. (Only if you want to use the RDF harvester)<br>
It is tested that ckanext-dcatde is working well with the release `v1.2.0` of ckanext-harvest.<br>

5. Enable the required plugins by adding to your CKAN configuration file:

       ckan.plugins = dcat dcatde harvest dcatde_rdf_harvester
       
   The plugins `harvest` and `dcatde_rdf_harvester` are only needed if you want to use the RDF harvester.

   In case you like the datasets to be indexed via [Google Dataset Search](https://toolbox.google.com/datasetsearch), activate the `structured_data` plugin in addition. See [ckanext-dcat README](https://github.com/ckan/ckanext-dcat/blob/master/README.md#structured-data-and-google-dataset-search-indexing) for details.


6. Add the following parameter to your CKAN configuration file to activate the additional profile for DCAT-AP.de:

       ckanext.dcat.rdf.profiles = euro_dcat_ap_2 dcatap_de

## RDF DCAT-AP.de Harvester
If the plugin `dcatde_rdf_harvester` is activated an additional source type `DCAT-AP.de RDF Harvester` is selectable.
The harvester supports the additional fields specified in DCAT-AP.de.

### Default license
By default the harvester will set a default license in the resource if in the resource of a dataset is no license
provided. In this case additional information about the harvest source, dataset and resource will be written
as log entry in the info level.

The value which will be used as default license can be defined by the
configuration parameter `ckanext.dcatde.harvest.default_license`. Add the following parameter to your CKAN configuration file, e.g.:

    ckanext.dcatde.harvest.default_license = http://dcat-ap.de/def/licenses/other-closed

### Skipping datasets which does not contain any resources
Skipping datasets which does not contain any resources can be activated by setting the optional
configuration parameter `resources_required` in the harvest source configuration.
Already existent datasets will not be skipped. Add the following parameter into the harvest source
configuration:

    {"resources_required": true}

### Cleaning Tags/Keywords
The DCAT-AP.de profile implements a different logic for cleaning tags/keywords as implemented in ckanext-dcat,
e.g. not replacing/removing German umlauts and 'ß'.

### Triplestore support
The originally harvested RDF data can also be written into a triplestore. It is tested with the Apache Jena
Fuseki (https://jena.apache.org/documentation/fuseki2/index.html).
To activate this option set the following configuration parameters.

    ckanext.dcatde.fuseki.triplestore.url = http://url/to/triplestore
    ckanext.dcatde.fuseki.triplestore.name = datastore_name
    ckanext.dcatde.fuseki.harvest.info.name = second_datastore_for_harvest_information

The triplestore application have to be installed and running before the first harvesting. In addition the
datastores have to be created manually.
`ckanext.dcatde.fuseki.triplestore.name` is the name of the datastore where the actual data is stored.
The datatstore `ckanext.dcatde.fuseki.harvest.info.name` is needed for the harvester to keep track of
information about the datasets so the current data will be updated properly when reharvesting.

#### SHACL support
If the triplestore is used you can also activate SHACL validation support by adding the following parameters.
It is tested with the SHACL-Validator from the ISA2 Interoperability Test Bed
(Sourcecode: https://github.com/ISAITB/shacl-validator,
Documentation: https://www.itb.ec.europa.eu/docs/guides/latest/validatingRDF)
The SHACL results will be stored in the triplestore.

    ckanext.dcatde.fuseki.shacl.store.name = shacl_output_datastore_name
    ckanext.dcatde.shacl_validator.api_url = http://url/to/shacl/validator/api
    ckanext.dcatde.shacl.validator.profile.type = shacl_profile_name

The SHACL validator application have to be installed and running before the first harvesting.

## Creating dcat-ap categories as groups
You need to add the following parameter to your CKAN configuration file:

    ckanext.dcatde.urls.themes = file:///path/to/file/dcat_theme.json

You will find an example file here: [dcat_theme.json](./examples/dcat_theme.json)
If you want to create the standard dcat-ap categories as groups you can use the ckan command "dcatde_themeadder" by following the instructions:

ON CKAN >= 2.9:

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini dcatde_themeadder

ON CKAN <= 2.8:

    (pyenv) $ paster --plugin=ckanext-dcatde dcatde_themeadder --config=/etc/ckan/default/production.ini

## Migrating ogd conform datasets to dcat-ap.de
You need to add the following parameter to your CKAN configuration file:

    ckanext.dcatde.urls.license_mapping = file:///path/to/file/dcat_license_mapping.json
    ckanext.dcatde.urls.category_mapping = file:///path/to/file/category_mapping.json

You will find the example files here: [dcat_license_mapping.json](./examples/dcat_license_mapping.json) and [category_mapping.json](./examples/category_mapping.json)
The migration requires that the dcat-ap categories exists as groups in CKAN, see [Creating dcat-ap categories as groups](#creating-dcat-ap-categories-as-groups).
If you want to migrate the datasets from ogd to dcat-ap.de you can use the ckan command "dcatde_migrate" by following the instructions:

ON CKAN >= 2.9:

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini dcatde_migrate

ON CKAN <= 2.8:

    (pyenv) $ paster --plugin=ckanext-dcatde dcatde_migrate --config=/etc/ckan/default/production.ini

With the version 3.1.1 an additional option to the migrate command was added to fix the migration of the OGD field `metadata_original_id`. Instead of mapping this field to `adms:identifier` it will be mapped to the field `dct:identifier` now.
The command can be executed as follows:

ON CKAN >= 2.9:

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini dcatde_migrate adms-id-migrate

ON CKAN <= 2.8:

    (pyenv) $ paster --plugin=ckanext-dcatde dcatde_migrate adms-id-migrate --config=/etc/ckan/default/production.ini

## Migrating contributor IDs in datasets
Adds the ContributorID defined in the related CKAN organization to the datasets if not alredy existent.
If the ContributorID of a contributor (see latest list at https://www.dcat-ap.de/def/contributors/), contributing
datasets to GovData, has changed, the ContributorID in the related datasets can be migrated. The list of the
deprecated ContributorIDs and the ContributorIDs replacing them are located in the source code.
The command can be executed as follows:

ON CKAN >= 2.9:

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini dcatde_migrate contributor-id-migrate

ON CKAN <= 2.8:

    (pyenv) $ paster --plugin=ckanext-dcatde dcatde_migrate contributor-id-migrate --config=/etc/ckan/default/production.ini

## Updating data in the triplestore
The data in the triplestore can be updated with several commands.

It is possible to reindex (adding or only updating) the manually maintained datasets.
The command can be executed as follows:

ON CKAN >= 2.9:

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini triplestore reindex

ON CKAN <= 2.8:

    (pyenv) $ paster --plugin=ckanext-dcatde triplestore reindex --config=/etc/ckan/default/production.ini

For some reasons it can be necessary to delete the data of one or more datasets in the triplestore.
The command can be executed as follows:

ON CKAN >= 2.9:

    (pyenv) $ ckan --config=/etc/ckan/default/production.ini triplestore delete_datasets {uris}

ON CKAN <= 2.8:

    (pyenv) $ paster --plugin=ckanext-dcatde triplestore delete_datasets {uris} --config=/etc/ckan/default/production.ini

`{uris}`: A comma separated list of URIs to delete from the triplestore

## Testing

Unit tests are placed in the `ckanext/dcatde/tests` directory and can be run with the pytest unit testing framework:

    $ cd /path/to/virtualenv/src/ckanext-dcatde
    $ pytest
