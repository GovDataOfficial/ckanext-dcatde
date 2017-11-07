# ckanext-govdatade

DCAT-AP.de specific CKAN extension for providing and importing DCAT-AP.de-Profile data.

**_This version of ckanex-dcatde is in BETA state. Please do not use it in production!_**

## Dependencies

The CKAN-Plugin ckanext-dcatde is based on the CKAN extension [ckanext-dcat](https://github.com/ckan/ckanext-dcat).

## Getting Started

If you are using Python virtual environment (virtualenv), activate it.

Install a specific version of the CKAN extension ckanext-dcat. It is tested that ckanext-dcatde is working well with the release `v0.0.6` of ckanext-dcat.

```bash
$ cd /path/to/virtualenv
$ /path/to/virtualenv/bin/pip install -e git+git://github.com/GovDataOfficial/ckanext-dcatde.git#egg=ckanext-dcatde
$ cd src/ckanext-dcatde
$ /path/to/virtualenv/bin/pip install -r base-requirements.txt -f requirements
$ python setup.py develop
```

Add the following plugins to your CKAN configuration file:

```ini
ckan.plugins = dcat dcatde
```

## Creating dcat-ap categories as groups
You need to add the following parameter to your CKAN configuration file:
```ini
ckanext.dcatde.urls.themes = file:///path/to/file/dcat_theme.json
```
You will find an example file here: [dcat_theme.json](./examples/dcat_theme.json)
If you want to create the standard dcat-ap categories as groups you can use the ckan command "dcatde_themeadder" by following the instructions:

    source /path/to/ckan/env/bin/activate
    sudo -u ckan /path/to/ckan/env/bin/paster --plugin=ckanext-dcatde dcatde_themeadder --config=/etc/ckan/default/production.ini

## Migrating ogd conform datasets to dcat-ap.de
You need to add the following parameter to your CKAN configuration file:
```ini
ckanext.dcatde.urls.license_mapping = file:///path/to/file/dcat_license_mapping.json
ckanext.dcatde.urls.category_mapping = file:///path/to/file/category_mapping.json
```
You will find the example files here: [dcat_license_mapping.json](./examples/dcat_license_mapping.json) and [category_mapping.json](./examples/category_mapping.json)
The migration requires that the dcat-ap categories exists as groups in CKAN, see [Creating dcat-ap categories as groups](#creating-dcat-ap-categories-as-groups).
If you want to migrate the datasets from ogd to dcat-ap.de you can use the ckan command "dcatde_migrate" by following the instructions:

    source /path/to/ckan/env/bin/activate
    sudo -u ckan /path/to/ckan/env/bin/paster --plugin=ckanext-dcatde dcatde_migrate --config=/etc/ckan/default/production.ini

## Testing

Unit tests are placed in the `ckanext/dcatde/tests` directory and can be run with the nose unit testing framework:

```bash
$ cd /path/to/virtualenv/src/ckanext-dcatde
$ nosetests
```
