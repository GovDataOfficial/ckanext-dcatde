'''
Migration functions for all fields that are migrated.
'''
import inspect
import json
from ckanext.dcatde.migration import util
import ckanext.dcatde.dataset_utils as ds_utils
from ckanext.dcatde.dataset_utils import EXTRA_KEY_HARVESTED_PORTAL


class MigrationFunctionExecutor(object):
    '''Use an instance of this class to easily apply all migration functions
    to a dataset.

    license_mapping_url: URL to a json file containing information on the
        license mapping. The file is the same as the one used for dcat-ap.de'''

    def __init__(self, license_mapping_url, category_mapping_url):
        self.functions = MigrationFunctions(
            self._get_license_mapping(license_mapping_url),
            self._get_category_mapping(category_mapping_url))

    def _get_license_mapping(self, license_mapping_url):
        '''Loads the license mapping from the given file URl'''
        file_content = util.load_json_mapping(license_mapping_url, "license")
        result = {}

        if 'list' in file_content:
            for item in file_content['list']:
                result[item['OGDLizenzcode']] = item['URI']

        return result

    def _get_category_mapping(self, category_mapping_url):
        '''Loads the category mapping from the given file URL'''
        return util.load_json_mapping(category_mapping_url, "category")

    def check_group_presence(self, ckan_group_dict):
        '''Checks if all groups from the category mapping are present
        in the given CKAN dict (obtained via API).
        Returns True if all groups are found, and False otherwise.'''
        for group in self.functions.new_groups:
            if group not in ckan_group_dict:
                util.get_migrator_log().error(u'Group ' + str(group)
                                              + u' not found. Did you run the '
                                              + u' theme adder command?')
                return False
        return True

    def apply_to(self, dataset):
        '''Applies all public migration functions (i.e. not starting with _)
        to the given dataset.
        If one a function fails, an error is logged and the next one is
        tried.'''
        for name, func in inspect.getmembers(self.functions, inspect.ismethod):
            if not name.startswith('_'):
                try:
                    func(dataset)
                except Exception:
                    util.get_migrator_log().error(
                        util.log_dataset_prefix(dataset) + 'Error applying ' +
                        name)


class MigrationFunctions(object):
    '''Holder for functions which are to be applied to a dataset.
    All of them only take the dataset as argument.

    license_mapping: JSON dict containing OGD license (key) <-> DCAT license URIs (value)
    category_mapping: JSON dict containing OGD groups (key) <-> DCAT themes (value)'''

    def __init__(self, license_mapping, category_mapping):
        self.license_mapping = license_mapping
        self.category_mapping = category_mapping

        # store a mapping which contains all new group names
        newgroups = []
        for key in category_mapping:
            item = category_mapping[key]
            if item:
                if isinstance(item, list):
                    newgroups = newgroups + item
                else:
                    newgroups.append(item)
        self.new_groups = set(newgroups)

    def metadata_original_portal(self, dataset):
        '''metadata_original_portal -> contributorID'''
        orig_field = ds_utils.get_extras_field(dataset, u'metadata_original_portal')
        target_field = ds_utils.get_extras_field(dataset, EXTRA_KEY_HARVESTED_PORTAL)

        if orig_field:
            util.rename_extras_field_migration(dataset, u'metadata_original_portal',
                                               u'contributorID', True, False)
            if target_field is None:
                ds_utils.insert_new_extras_field(dataset, EXTRA_KEY_HARVESTED_PORTAL,
                                                 orig_field['value'], False)

    def metadata_original_id(self, dataset):
        '''metadata_original_id -> extras.identifier'''
        util.rename_extras_field_migration(dataset, u'metadata_original_id',
                                           u'identifier', False, False)

    def spatial_reference_text(self, dataset):
        '''spatial_reference.text -> extras.geocodingText'''
        spatial_reference = ds_utils.get_extras_field(dataset, 'spatial_reference')
        if spatial_reference is not None:
            sr_value = spatial_reference['value']
        else:
            sr_value = None

        if sr_value is not None:
            # Convert string representation of dictionary to actual dictionary
            sr_value_dict = json.loads(sr_value)
            field = sr_value_dict.get('text')

            if field is not None:
                ds_utils.insert_new_extras_field(dataset, u'geocodingText',
                                                 field, True)

                sr_value_dict.pop('text', None)
                spatial_reference['value'] = str(json.dumps(sr_value_dict,
                                                                sort_keys=True))

    def groups(self, dataset):
        if 'groups' in dataset:
            for group_name in [n['name'] for n in dataset['groups']]:  # iterate list of groupnames
                if group_name in list(self.category_mapping.keys()):
                    themes = self.category_mapping[group_name]

                    # remove old group_name
                    util.delete_group(dataset, group_name)

                    # transform single strings to lists with one argument
                    if isinstance(themes, str):
                        themes = [themes]

                    if themes is not None:
                        for theme in themes:
                            dataset['groups'].append({'id': theme, 'name': theme})
                elif group_name not in self.new_groups:
                    util.log_error(dataset, u'INVALID: non-OGD-Category found: ' + str(group_name))

    def temporal_coverage_from(self, dataset):
        '''temporal_coverage_from -> temporal_start'''
        util.rename_extras_field_migration(dataset, u'temporal_coverage_from',
                                           u'temporal_start', False, False)

    def temporal_coverage_to(self, dataset):
        '''temporal_coverage_to -> temporal_end'''
        util.rename_extras_field_migration(dataset, u'temporal_coverage_to',
                                           u'temporal_end', False, False)

    def geographical_granularity(self, dataset):
        '''geographical_granularity -> politicalGeocodingLevelUri'''
        valid_values = {'bund': 'federal', 'land': 'state',
                        'kommune': 'municipality', 'stadt': 'municipality',

                        # DCAT values (without URI part) stay the same
                        'federal': 'federal', 'state': 'state',
                        'municipality': 'municipality',

                        # Additional non-OGD value
                        'kreis': 'administrativeDistrict'}

        geo_level = ds_utils.get_extras_field(dataset, 'geographical_granularity')
        target_field = ds_utils.get_extras_field(dataset,
                                                 u'politicalGeocodingLevelURI')

        # only add if the field hasn't been migrated before
        if target_field is None:
            if geo_level is not None:
                geo_level_value = geo_level['value'].lower()

                if geo_level_value in valid_values:
                    geo_level_value = ('http://dcat-ap.de/def/politicalGeocoding/Level/'
                                       + valid_values.get(geo_level_value))
                else:
                    util.log_error(dataset,
                                   'INVALID: politicalGeocodingLevelURI: ' + geo_level_value)

                geo_level['value'] = geo_level_value

                util.rename_extras_field_migration(dataset, u'geographical_granularity',
                                                   u'politicalGeocodingLevelURI', False)

    def contacts_role_autor(self, dataset):
        '''contacts.role.autor -> extras.author'''
        fields = util.get_extras_contacts_data(dataset, 'autor')
        target_field = ds_utils.get_extras_field(dataset, u'author_contacttype')

        # only add if the field hasn't been migrated before (check for added field)
        if target_field is None:
            if fields is not None:
                if fields.get('name') and fields.get('email'):
                    dataset['author'] = fields.pop('name', '')
                    dataset['author_email'] = fields.pop('email', '')
                    ds_utils.insert_new_extras_field(dataset, 'author_url',
                                                     fields.pop('url', ''), False)

                    util.update_extras_contacts_data(dataset, 'autor', fields)

                    # Additional field
                    ds_utils.insert_new_extras_field(dataset, u'author_contacttype',
                                                     u'Organization', False)

                util.move_extras_contacts_address(dataset, 'autor', 'author',
                                                  fields)

    def contacts_role_ansprechpartner(self, dataset):
        '''contacts.role.ansprechpartner -> extras.maintainer'''
        fields = util.get_extras_contacts_data(dataset, 'ansprechpartner')
        target_field = ds_utils.get_extras_field(dataset,
                                                 u'maintainer_contacttype')

        # only add if the field hasn't been migrated before (check for added field)
        if target_field is None:
            if fields is not None:
                if fields.get('name') and fields.get('email'):
                    dataset['maintainer'] = fields.pop('name', '')
                    dataset['maintainer_email'] = fields.pop('email', '')
                    ds_utils.insert_new_extras_field(dataset, u'maintainer_url',
                                                     fields.pop('url', ''), False)

                    util.update_extras_contacts_data(dataset, 'ansprechpartner',
                                                     fields)

                    # Additional field
                    ds_utils.insert_new_extras_field(dataset, u'maintainer_contacttype',
                                                     u'Organization', False)

                util.move_extras_contacts_address(dataset, 'ansprechpartner',
                                                  'maintainer', fields)

    def contacts_role_veroeffentlichende_stelle(self, dataset):
        '''contacts.role.veroeffentlichende_stelle -> extras.publisher'''
        fields = util.get_extras_contacts_data(dataset, 'veroeffentlichende_stelle')
        target_field = ds_utils.get_extras_field(dataset,
                                                 u'publisher_contacttype')

        # only add if the field hasn't been migrated before (check for added field)
        if target_field is None:
            if fields is not None:
                ds_utils.insert_new_extras_field(dataset, u'publisher_name',
                                                 fields.pop('name', ''), False)
                ds_utils.insert_new_extras_field(dataset, u'publisher_email',
                                                 fields.pop('email', ''), False)
                ds_utils.insert_new_extras_field(dataset, u'publisher_url',
                                                 fields.pop('url', ''), False)

                util.update_extras_contacts_data(dataset,
                                                 'veroeffentlichende_stelle',
                                                 fields)

                # Additional field
                ds_utils.insert_new_extras_field(dataset, u'publisher_contacttype',
                                                 u'Organization', False)
                util.move_extras_contacts_address(dataset, 'veroeffentlichende_stelle',
                                              'publisher', fields)

    def license_id(self, dataset):
        '''Add license ID to every resource'''
        resources = dataset['resources']
        license_id_data = dataset['license_id']

        if not license_id_data:
            util.log_error(dataset, "No license_id")
            return

        # key == OGD value and value == DCAT URI
        if license_id_data in list(self.license_mapping.keys()):
             # OGD value is present. Map it accordingly.
            license_id_dcat = self.license_mapping[license_id_data]
            dataset['license_id'] = license_id_dcat
        elif license_id_data not in list(self.license_mapping.values()):
            # Invalid value, neither OGD nor DCAT
            util.log_error(dataset, u"license_id '{}' not part of the mapping".format(
                str(license_id_data)))
            return

        # At this point, a valid DCAT license is present in dataset['license_id'].

        if resources is not None:
            for resource in resources:
                resource['license'] = dataset['license_id']

    def terms_of_use_attribution_text(self, dataset):
        '''
        Add attribution text to every resource
        handles: dataset['extras']['terms_of_use']: "{\"attribution_text\": \"bla\"}"
        '''
        fieldname = u'terms_of_use'
        resources = dataset['resources']

        terms_of_use = ds_utils.get_extras_field(dataset, fieldname)
        if terms_of_use is not None:
            text = json.loads(terms_of_use.get('value')).get('attribution_text')

            if text and resources:
                for resource in resources:
                    resource['licenseAttributionByText'] = text

    def dates_role_veroeffentlicht(self, dataset):
        '''extras.dates.role = "veroeffentlicht" -> extras.issued'''
        util.migrate_dates_field(dataset, 'veroeffentlicht',
                                 'issued')

    def dates_role_aktualisiert(self, dataset):
        '''extras.dates.role = "aktualisiert" -> extras.modified'''
        util.migrate_dates_field(dataset, 'aktualisiert',
                                 'modified')

    def languages(self, dataset):
        '''convert ISO 639-1 language codes to DCAT-AP conform URIs (containing ISO 639-3 codes'''
        field_name = u'language'

        # dataset
        language_field = ds_utils.get_extras_field(dataset, field_name)
        if language_field:
            util.update_language_in(dataset, language_field, 'value', 'language')

        # resources
        if 'resources' in dataset and dataset['resources']:
            for resource in dataset['resources']:
                if resource.get(field_name):
                    util.update_language_in(dataset, resource, field_name,
                                            'Resource->language')
