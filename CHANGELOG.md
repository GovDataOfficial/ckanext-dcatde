# Changelog

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
