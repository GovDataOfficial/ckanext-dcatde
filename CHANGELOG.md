# Changelog

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