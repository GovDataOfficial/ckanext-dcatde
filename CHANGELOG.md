# Changelog

## v6.1.0 2023-08-01

* Updates and cleans up dependencies
* Standardization of the `test.ini` file

## v6.0.0 2023-07-05

* Removes support for old CKAN versions prior 2.9 and Python 2

## v5.16.0 2023-06-29

* Updates ckanext-dcat to version 1.5.1 and removes support for all properties in class dcat:DataService
  except for `dcatde:licenseAttributionByText`, as these are now supported directly in ckanext-dcat.
* Updates ckanext-harvest to version 1.5.5
* Fixes SA warnings from sqlalchemy that occur since version 1.4

## v5.13.0 2023-05-04

* Adds support for CKAN 2.10.0

## v5.11.0 2023-03-16

* Improves duplicate detection: Adds support for setting a priority to the harvester configuration. The
  remote dataset is imported if the modified dates of the remote and local dataset are equal and for the
  harvester of the remote dataset was specified a higher priority than for the harvester of the local dataset.

## v5.9.0 2023-01-23

* Fixes tests for Python 2
* Updates ckanext-harvest to version 1.4.2
* Adds support for Python 3.9

## v5.8.0 2022-12-15

* Updates ckanext-dcat to version 1.4.0
* Remove pinning version for cryptography dependency
* Updates ckanext-harvest to version 1.4.1
* Adds support for property dcat:accessService in class dcat:Distribution
* Add last_modified as fallback value to resource modified date
* Add support for property dcatde:licenseAttributionByText in class dcat:DataService

## v5.6.0 2022-11-03

* Fixes correlation when parsing additionally contact point properties
* Updates pylint configuration to latest version and fixes several warnings

## v5.5.0 2022-10-20

* Adds support for property dct:references in class dcat:Dataset

## v5.4.0 2022-09-12

* Fixes string assertion error with Python 2
* Fixes test for invalid URIRef
* Internal changes: Switches Python environment from Python 3.6 to Python 3.8

## v5.3.0 2022-08-04

* Improves the referencing of distribution when adding DCAT-AP.de properties
* Adds support for property dcatap:availability in class dcat:Dataset
* Updates ckanext-dcat to version 1.3.0

## v5.2.0 2022-05-31

* Updates ckanext-harvest to version 1.4.0 and ckanext-dcat to version 1.2.0
* Updates deprecated contributor IDs in migration command

## v5.1.2 2022-05-05

* Fixes saving resource extras in the migration functions and in the DCAT-AP.de profile
* Fixes clear harvest source history command call in cronjob shell script
* Fixes CKAN action context DCAT-AP.de migration command

## v5.1.0 2022-04-07

* Support for Python 3

## v5.0.1 2022-03-31

* Updates ckanext-harvest to version 1.3.4
* Updates deprecated contributor IDs in migration command

## v4.7.0 2022-02-17

* Skip saving harvested datasets in the triple store without distribution and harvest source config param
  `resources_required: true` is set
* Reads the ContributorID from the harvester config and add it to the dataset graph if not already present
* Saves ContributorID in addition to the organizationID in the triple store with the validation results
* Adds ContributorID from the harvester config to CKAN dataset if not already present
* Ensure that tags keep minimum length after normalization
* Fixes harvest object state. Marks corresponding harvest objects as not current when deleting duplicate
  datasets and datasets which contain no resources anymore

## v4.6.6 2022-01-27

* Deletes deprecated datasets in CKAN regardless of whether the dataset could previously be renamed or not 
* Deletes deprecated datasets from triple store even if there is no owner org defined in the harvest source

## v4.6.4 2021-12-21

* Adds the option `--keep-current=true` to the clear harvest jobs shell script.

## v4.6.3 2021-12-16

* Fixes dev-requirements.txt: Broken version 1.7.0 of lazy-object-proxy was banned
* Fixes saving information about the harvested datasets saved in the triple store if there is more than one
  harvest source linked to the same organisation

## v4.6.2 2021-11-23

* Explicitly disallow incorrect version of python-dateutil
* Updates requirement ckanext-harvest to internal version 1.3.3.dev1

## v4.6.1 2021-11-15

* Updates requirement ckanext-dcat to official version 1.1.3

## v4.6.0 2021-11-04

* Updates the README.md with information about the support of a triplestore and a SHACL validation
* Adds the option to delete specific dataset by their URIs from the triplestore to the ckan command "triplestore"

## v4.5.8 2021-10-14

* Updates requirement ckanext-dcat to fix publisher URI handling

## v4.5.7 2021-10-07

* Updates requirement ckanext-dcat to support URIRef values in "rights" and "accessRights"
* Changes the serialization format for metadata from "xml" to "turtle", because it is more strict and fails
  if URIRef elements contain invalid characters

## v4.5.4 2021-09-10

* Limit size of harvesting fetch and gather consumer log files

## v4.4.3 2021-06-22

* Updates requirement ckanext-dcat to version 1.1.2
* Adds the possibility to update the contributorID for manually maintained datasets when the contributorID
  has changed

## v4.4.1 2021-04-28

* Make requirement for ckanext-harvest optional (#10)

## v4.4.0 2021-03-26

* Adds new migration script option `contributor-id-migrate` to add the contributorID to existing manually
  maintained datasets
* Adds SHACL validation support to the triplestore ckan command
* Introduce the possiblity to validate the dataset graph by SHACL when updating the dataset and save the
  validation result in a triplestore
* Handle requests exceptions if the triplestore endpoint is not reachable

## v4.3.0 2021-01-19

* Also deletes data in triplestore when dataset is deleted in CKAN
* Adds the triplestore ckan command

## v4.2.3 2020-12-17

* Improve logging messages in duplicate detection
* Improve logging when updating data in the triplestore
* Remove pinning version for cryptography dependency. Version >=3.3.1 is working again.

## v4.2.2 2020-12-13

* Improve exception handling when updating data in the triplestore
* Pin version for cryptography dependency avoiding build errors with version >=3.3

## v4.2.0 2020-12-08

* Implemented: Add harvested data into a triplestore

## v4.0.0 2020-10-30

* Avoid crashes of the fetch consumer in case deletion harvest objects are corrupted
* Fixed problem with python dependency 'pycountry' that caused the build to fail.

## v3.9.0 2020-03-26

* When remote datasets without resources/distributions are rejected (`resources_required`), any local version of
  the dataset is deleted if present.
* Fix line endings to match .gitattributes
* Fix harvester plugin docs (#11)

## v3.8.0 2020-02-17

* Update requirement ckanext-dcat to version 1.1.0
* Catch exception if 'email-validator' is not available in older CKAN versions
* Remove patch disabling SSL verification for older Python 2.7 versions
* Adds support for the different VCARD representations for DCAT.contactPoint

## v3.7.0 2019-12-19

* Update version for requirements ckanext-harvest and ckanext-dcat
* Remove the restriction to a specific version of CKAN
* Fix in RDF profile: Remove prefix "mailto:" from values in fields containing an email address in method parse_dataset
* Change in DCAT-AP.de RDF harvester: Remove validator 'email-validator' from create/update package schema

## v3.6.0 2019-11-05

* Improve logic of the duplicate detection and add deletion of older duplicates within the duplicate detection
* Map older licenses in resources from DCAT-AP.de version v1.0 to the latest version v1.0.2

## v3.5.4 2019-10-14

* Improve comparing dates with and without time zone information used by the duplicate detection

## v3.5.2 2019-09-05

* Add different implementation for cleaning tags/keywords
* Add harvest source configuration `resources_required`, which logs and skips all datasets without distributions (CKAN resources)

## v3.5.1 2019-07-23

* Fix possible error in logging message when setting default license

## v3.5.0 2019-07-18

* Add support for class FOAF.Agent as rdf:type in dcatde:originator, dcatde:maintainer, dct:contributor and
  dct:creator
* Set default license (`http://dcat-ap.de/def/licenses/other-closed`) in the resources of a
  dataset if no license is provided and write a log entry with additional information about the harvest
  source, dataset and resource in the info level. Introduce configuration parameter
  `ckanext.dcatde.harvest.default_license` for defining the default license.
* Serialize dcatde:contributorID as type UriRef if the value is an URI, otherwise as Literal
* Rename environment names for internal ci/cd pipeline

## v3.3.0 2019-03-12

* Update ckanext-dcat to v0.0.9
* Update ckanext-harvest to v1.1.4
* Remove patches (Fixes #6)
* Delete requirements subfolder which contained pre-built wheels
* Add supervisor config for harvesting `gather_consumer` and `fetch_consumer`
* Add cronjob scripts to run and clear harvest jobs. These scripts are used with GovData and were previously
  included in ckanext-govdatade.
* Add support for dct:type in dcatde:originator, dcatde:maintainer, dct:contributor and dct:creator

## v3.2.0 2018-12-21

* The profile and examples now use the DCAT-AP.de v1.0.1 Namespace
    * Renamed `legalbasisText` to `legalBasis` and `geocodingText` to `geocodingDescription`
* Added logic to parse older DCAT-AP.de Namespaces
* Improved dct:format and dcat:mediaType handling
* Improved selecting of the default language

## v3.1.3 2018-11-09

* Fix problem with not deleting metadata without guid while harvesting
* Fix handling of downloadURL and accessURL
* Select title, description and names in the default language if available
* Fix error in in graph_from_dataset() if there is no contactPoint exists in the graph

## v3.1.2 2018-05-18

* Updated the examples for the licenses in CKAN and the license mapping to DCAT-AP.de v1.0.1
* Updated the example for the RDF endpoint to DCAT-AP.de v1.0.1
* Added patch for DCAT harvester that it uses the default `_get_user_name` logic of ckanext-harvest
* Added patch for ckanext-harvest that the default dataset name suffix is configurable

## v3.1.1 2018-03-29

* OGD `metadata_original_id` is now mapped to `dct:identifier` instead of `adms:identifier`
    * Added new migration script option `adms-id-migrate` to fix existing DCAT-AP.de datasets
* Correctly set `metadata_harvested_portal` for the custom RDF Harvester

## v3.1.0 2018-02-28

* Avoiding an invalid rdf graph because of whitespaces in URIRef values by removing whitespaces before adding URIRef objects into the graph
* Added DCAT-AP.de specific RDF Harvester
* The dependency to ckanext-harvest was added
* Harmonized the version between the other CKAN-Plugins of GovData

## v1.0.1 2017-12-21

* Initial version of the CKAN plugin
    * Extends the Output-Mapping about the DCAT-AP.de specific fields
    * Contains a script (CKAN paster command) to create CKAN groups from the DCAT-AP categories
    * Contains a migration script (CKAN paster command) to migrate the datasets in the CKAN database from OGD to DCAT-AP.de structure
    * Contains a shell script to purge the CKAN groups representing the OGD categories
