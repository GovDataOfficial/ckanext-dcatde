# Changelog

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