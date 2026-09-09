# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [6.12.0] - 2026-09-04

### Added
- Assign datasets to publishers in the DCAT-AP.de RDF harvester ([#22](https://github.com/GovDataOfficial/ckanext-dcatde/pull/22))
- Configurable spatial vertex limit via `_MAX_SPATIAL_VERTICES_DEFAULT`; oversized geodata is simplified via `simplified_geojson_str`

### Changed
- Compatibility with latest setuptools (temporary pin `setuptools<81`)
- Pin `pyLD` to a version < 3.0.0 for Python < 3.10

### Fixed
- Huge geodata no longer cause errors during harvesting
- Stabilized deb package removal

## [6.11.1] - 2026-03-25

### Fixed
- [#39](https://github.com/GovDataOfficial/ckanext-dcatde/issues/39)

## [6.11.0] - 2025-12-15

### Changed
- Group id generation is compatible with CKAN 2.11 ([#33](https://github.com/GovDataOfficial/ckanext-dcatde/issues/33))

## [6.10.1] - 2025-08-28

### Changed
- Deprecated contributorID from `bundesministeriumFuerWirtschaftUndKlimaschutz` to `bundesministeriumFuerWirtschaftUndEnergie`

## [6.10.0] - 2025-07-28

### Changed
- Duplicate detection now uses the CKAN database instead of the solr index to avoid data inconsistencies ([#30](https://github.com/GovDataOfficial/ckanext-dcatde/issues/30))
- Harvester config option `harvested_portal` is now optional; README updated ([#27](https://github.com/GovDataOfficial/ckanext-dcatde/issues/27))

## [6.9.0] - 2025-03-24

### Changed
- `DCATdeProfile` now inherits from `EuropeanDCATAP2Profile` instead of `RDFProfile`. **Notice:** requires removing `euro_dcat_ap_2` from `ckanext.dcat.rdf.profiles` in the CKAN configuration, see [README](./README.md#getting-started)
- Updated ckanext-dcat to 2.3.0
- Updated README with info about `ckanext.dcatde.urls.dcat_licenses_upgrade_mapping` ([#28](https://github.com/GovDataOfficial/ckanext-dcatde/issues/28))

### Fixed
- Processing of `DCAT.contactPoint` ([#24](https://github.com/GovDataOfficial/ckanext-dcatde/issues/24))

## [6.8.3] - 2024-08-26

### Added
- Error handling for the `SearchIndexError` exception

## [6.8.0] - 2024-04-25

### Changed
- Updated ckanext-dcat to 1.7.0

## [6.7.0] - 2024-03-26

### Changed
- Updated ckanext-dcat to 1.6.0

## [6.4.2] - 2023-11-07

### Changed
- The `ckanext-harvest` extension is no longer required to use the DCAT-AP.de RDF profile

## [6.1.0] - 2023-08-01

### Changed
- Updated and cleaned up dependencies
- Standardization of the `test.ini` file

## [6.0.0] - 2023-07-05

### Removed
- Support for CKAN versions prior 2.9 and Python 2

## [5.16.0] - 2023-06-29

### Changed
- Updated ckanext-dcat to 1.5.1
- Updated ckanext-harvest to 1.5.5

### Removed
- All properties in class `dcat:DataService` except `dcatde:licenseAttributionByText` (now supported directly in ckanext-dcat)

### Fixed
- SA warnings from SQLAlchemy that occur since version 1.4

## [5.13.0] - 2023-05-04

### Added
- Support for CKAN 2.10.0

## [5.11.0] - 2023-03-16

### Changed
- Duplicate detection supports a harvester priority: the remote dataset is imported when modified dates are equal and the remote harvester has a higher priority

## [5.9.0] - 2023-01-23

### Added
- Support for Python 3.9

### Changed
- Updated ckanext-harvest to 1.4.2

### Fixed
- Tests for Python 2

## [5.8.0] - 2022-12-15

### Added
- Support for property `dcat:accessService` in class `dcat:Distribution`
- `last_modified` as fallback value to resource modified date
- Support for property `dcatde:licenseAttributionByText` in class `dcat:DataService`

### Changed
- Updated ckanext-dcat to 1.4.0
- Updated ckanext-harvest to 1.4.1
- Removed pinning version for cryptography dependency

## [5.6.0] - 2022-11-03

### Changed
- Updated pylint configuration to latest version and fixed several warnings

### Fixed
- Correlation when parsing additional contact point properties

## [5.5.0] - 2022-10-20

### Added
- Support for property `dct:references` in class `dcat:Dataset`

## [5.4.0] - 2022-09-12

### Changed
- Internal: switched Python environment from 3.6 to 3.8

### Fixed
- String assertion error with Python 2
- Test for invalid URIRef

## [5.3.0] - 2022-08-04

### Added
- Support for property `dcatap:availability` in class `dcat:Dataset`

### Changed
- Improved the referencing of distribution when adding DCAT-AP.de properties
- Updated ckanext-dcat to 1.3.0

## [5.2.0] - 2022-05-31

### Changed
- Updated ckanext-harvest to 1.4.0 and ckanext-dcat to 1.2.0
- Updated deprecated contributor IDs in migration command

## [5.1.2] - 2022-05-05

### Fixed
- Saving resource extras in the migration functions and in the DCAT-AP.de profile
- Clear harvest source history command call in cronjob shell script
- CKAN action context DCAT-AP.de migration command

## [5.1.0] - 2022-04-07

### Added
- Support for Python 3

## [5.0.1] - 2022-03-31

### Changed
- Updated ckanext-harvest to 1.3.4
- Updated deprecated contributor IDs in migration command

## [4.7.0] - 2022-02-17

### Added
- Read the ContributorID from the harvester config and add it to the dataset graph if not already present
- Save ContributorID in addition to the organizationID in the triple store with the validation results
- Add ContributorID from the harvester config to CKAN dataset if not already present

### Changed
- Skip saving harvested datasets in the triple store without distribution when `resources_required: true` is set
- Ensure that tags keep minimum length after normalization

### Fixed
- Harvest object state: mark corresponding harvest objects as not current when deleting duplicates and datasets without resources

## [4.6.6] - 2022-01-27

### Changed
- Delete deprecated datasets in CKAN regardless of whether the dataset could previously be renamed or not
- Delete deprecated datasets from triple store even if there is no owner org defined in the harvest source

## [4.6.4] - 2021-12-21

### Added
- `--keep-current=true` option to the clear harvest jobs shell script

## [4.6.3] - 2021-12-16

### Fixed
- `dev-requirements.txt`: banned broken version 1.7.0 of `lazy-object-proxy`
- Saving information about harvested datasets in the triple store if there is more than one harvest source linked to the same organisation

## [4.6.2] - 2021-11-23

### Changed
- Explicitly disallow incorrect version of `python-dateutil`
- Updated requirement ckanext-harvest to internal version 1.3.3.dev1

## [4.6.1] - 2021-11-15

### Changed
- Updated requirement ckanext-dcat to official version 1.1.3

## [4.6.0] - 2021-11-04

### Added
- Option to delete specific datasets by their URIs from the triplestore in the ckan command "triplestore"

### Changed
- Updated the README.md with information about the support of a triplestore and a SHACL validation

## [4.5.8] - 2021-10-14

### Changed
- Updated requirement ckanext-dcat to fix publisher URI handling

## [4.5.7] - 2021-10-07

### Changed
- Updated requirement ckanext-dcat to support URIRef values in `rights` and `accessRights`
- Changed the serialization format for metadata from `xml` to `turtle` (stricter and fails on invalid URIRef characters)

## [4.5.4] - 2021-09-10

### Changed
- Limit size of harvesting fetch and gather consumer log files

## [4.4.3] - 2021-06-22

### Added
- Possibility to update the contributorID for manually maintained datasets when the contributorID has changed

### Changed
- Updated requirement ckanext-dcat to 1.1.2

## [4.4.1] - 2021-04-28

### Changed
- Make requirement for ckanext-harvest optional (#10)

## [4.4.0] - 2021-03-26

### Added
- New migration script option `contributor-id-migrate` to add the contributorID to existing manually maintained datasets
- SHACL validation support to the triplestore ckan command
- Possibility to validate the dataset graph by SHACL when updating the dataset and save the validation result in a triplestore

### Fixed
- Handle requests exceptions if the triplestore endpoint is not reachable

## [4.3.0] - 2021-01-19

### Added
- `triplestore` ckan command

### Changed
- Also delete data in triplestore when dataset is deleted in CKAN

## [4.2.3] - 2020-12-17

### Changed
- Improved logging messages in duplicate detection
- Improved logging when updating data in the triplestore
- Removed pinning version for cryptography dependency (>=3.3.1 is working again)

## [4.2.2] - 2020-12-13

### Changed
- Pin version for cryptography dependency avoiding build errors with version >=3.3

### Fixed
- Improved exception handling when updating data in the triplestore

## [4.2.0] - 2020-12-08

### Added
- Add harvested data into a triplestore

## [4.0.0] - 2020-10-30

### Fixed
- Avoid crashes of the fetch consumer in case deletion harvest objects are corrupted
- Problem with python dependency `pycountry` that caused the build to fail

## [3.9.0] - 2020-03-26

### Changed
- When remote datasets without resources/distributions are rejected (`resources_required`), any local version of the dataset is now deleted if present

### Fixed
- Line endings to match `.gitattributes`
- Harvester plugin docs (#11)

## [3.8.0] - 2020-02-17

### Added
- Support for the different VCARD representations for `DCAT.contactPoint`

### Changed
- Updated requirement ckanext-dcat to 1.1.0

### Removed
- Patch disabling SSL verification for older Python 2.7 versions

### Fixed
- Catch exception if `email-validator` is not available in older CKAN versions

## [3.7.0] - 2019-12-19

### Changed
- Updated version for requirements ckanext-harvest and ckanext-dcat
- Removed the restriction to a specific version of CKAN
- DCAT-AP.de RDF harvester: removed validator `email-validator` from create/update package schema

### Fixed
- RDF profile: remove prefix `mailto:` from values in fields containing an email address in method `parse_dataset`

## [3.6.0] - 2019-11-05

### Changed
- Improved logic of the duplicate detection and added deletion of older duplicates within the duplicate detection
- Map older licenses in resources from DCAT-AP.de v1.0 to v1.0.2

## [3.5.4] - 2019-10-14

### Changed
- Improved comparing dates with and without time zone information used by the duplicate detection

## [3.5.2] - 2019-09-05

### Added
- Harvest source configuration `resources_required`, which logs and skips all datasets without distributions (CKAN resources)

### Changed
- Different implementation for cleaning tags/keywords

## [3.5.1] - 2019-07-23

### Fixed
- Possible error in logging message when setting default license

## [3.5.0] - 2019-07-18

### Added
- Support for class `FOAF.Agent` as `rdf:type` in `dcatde:originator`, `dcatde:maintainer`, `dct:contributor` and `dct:creator`
- Default license (`http://dcat-ap.de/def/licenses/other-closed`) in the resources of a dataset if no license is provided; configurable via `ckanext.dcatde.harvest.default_license`

### Changed
- Serialize `dcatde:contributorID` as type UriRef if the value is a URI, otherwise as Literal
- Renamed environment names for internal CI/CD pipeline

## [3.3.0] - 2019-03-12

### Added
- Supervisor config for harvesting `gather_consumer` and `fetch_consumer`
- Cronjob scripts to run and clear harvest jobs (previously included in ckanext-govdatade)
- Support for `dct:type` in `dcatde:originator`, `dcatde:maintainer`, `dct:contributor` and `dct:creator`

### Changed
- Updated ckanext-dcat to 0.0.9
- Updated ckanext-harvest to 1.1.4

### Removed
- Patches (fixes #6)
- Requirements subfolder which contained pre-built wheels

## [3.2.0] - 2018-12-21

### Added
- Logic to parse older DCAT-AP.de namespaces

### Changed
- The profile and examples now use the DCAT-AP.de v1.0.1 namespace; renamed `legalbasisText` to `legalBasis` and `geocodingText` to `geocodingDescription`
- Improved `dct:format` and `dcat:mediaType` handling
- Improved selecting of the default language

## [3.1.3] - 2018-11-09

### Changed
- Select title, description and names in the default language if available

### Fixed
- Not deleting metadata without guid while harvesting
- Handling of downloadURL and accessURL
- Error in `graph_from_dataset()` if there is no contactPoint in the graph

## [3.1.2] - 2018-05-18

### Added
- Patch for DCAT harvester so it uses the default `_get_user_name` logic of ckanext-harvest
- Patch for ckanext-harvest so the default dataset name suffix is configurable

### Changed
- Updated the examples for the licenses in CKAN and the license mapping to DCAT-AP.de v1.0.1
- Updated the example for the RDF endpoint to DCAT-AP.de v1.0.1

## [3.1.1] - 2018-03-29

### Added
- New migration script option `adms-id-migrate` to fix existing DCAT-AP.de datasets

### Changed
- OGD `metadata_original_id` is now mapped to `dct:identifier` instead of `adms:identifier`

### Fixed
- Correctly set `metadata_harvested_portal` for the custom RDF Harvester

## [3.1.0] - 2018-02-28

### Added
- DCAT-AP.de specific RDF Harvester
- Dependency to ckanext-harvest

### Changed
- Harmonized the version with the other CKAN plugins of GovData

### Fixed
- Avoid an invalid RDF graph caused by whitespaces in URIRef values by removing whitespaces before adding URIRef objects into the graph

## [1.0.1] - 2017-12-21

### Added
- Initial version of the CKAN plugin
- Extends the output mapping with DCAT-AP.de specific fields
- Script (CKAN paster command) to create CKAN groups from the DCAT-AP categories
- Migration script (CKAN paster command) to migrate the datasets in the CKAN database from OGD to DCAT-AP.de structure
- Shell script to purge the CKAN groups representing the OGD categories
