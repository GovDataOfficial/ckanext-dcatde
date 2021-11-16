'''
Migration helper functions
'''
import json
import logging
import re
import urllib2
import pycountry
import ckanext.dcatde.dataset_utils as ds_utils


def log_dataset_prefix(dataset):
    if isinstance(dataset, dict) and 'name' in dataset:
        return 'Dataset ' + dataset['name'] + ': '
    return ''


def get_migrator_log():
    return logging.getLogger('ckanext.dcatde.migration')


def log_warn(dataset, message):
    get_migrator_log().warn(log_dataset_prefix(dataset) + message)


def log_error(dataset, message):
    get_migrator_log().error(log_dataset_prefix(dataset) + message)


def load_json_mapping(url, errorhint, logger=None):
    '''Loads the Mapping from the given file URL'''
    if logger is None:
        logger = get_migrator_log()
    try:
        logger.debug("Trying to open: " + url)
        return json.loads(urllib2.urlopen(url).read())
    except Exception:
        logger.error('Could not load ' + errorhint + ' mapping')
        return {}


def rename_extras_field_migration(dataset, name_old, name_new, as_list, do_log=False):
    result = ds_utils.rename_extras_field(dataset, name_old, name_new, as_list)

    if do_log and not result:
        log_warn(dataset, "Field '%s' was not found" % name_old)


def delete_group(dataset, name):
    # find the group by it's name
    for group in dataset['groups']:
        if group['name'] == name:
            dataset['groups'].remove(group)
            return


def get_extras_json_list_data(dataset, extras_field, check_key, expected_val):
    '''Gets data from extras_field. The field is expected to contain a list of
    dicts as JSON string.
    This method returns the deserialized list entry having expected_val in
    check_key, or None if no such element exists.'''
    fld_content = ds_utils.get_extras_field(dataset, extras_field)

    if fld_content is not None:
        fld_list = json.loads(fld_content['value'], encoding='utf-8')
        for entry in fld_list:
            if entry.get(check_key) == expected_val:
                return entry

    return None


def update_extras_json_list_data(dataset, extras_field, check_key, expected_val, content):
    '''Updates an extras value with key extras_field. This field is expected to
    contain a list of dicts as JSON string.
    The method updates the content of the list entry having
    check_key: expected_val.
    If the given content is nonempty, and if it contains more key-value
    pairs than the checked pair, the data is updated.
    Otherwise, the dict is dropped from the list.
    If the whole list became empty, the extras field is dropped.'''
    fld_content = ds_utils.get_extras_field(dataset, extras_field)

    if fld_content is not None:
        fld_list = json.loads(fld_content['value'], encoding='utf-8')

        for index, entry in enumerate(fld_list):
            if entry.get(check_key) == expected_val:
                # update only if there are additional values given,
                # otherwise drop the entry
                if content and content != {check_key: expected_val}:
                    fld_list[index] = content
                else:
                    del fld_list[index]
        if fld_list:
            fld_content['value'] = unicode(json.dumps(fld_list, sort_keys=True))
        else:
            # drop contacts if it became empty
            ds_utils.delete_extras_field(dataset, extras_field)
    else:
        log_warn(dataset, 'Could not update data, no field "' +
                 extras_field + '" in extras')


def get_extras_contacts_data(dataset, role):
    return get_extras_json_list_data(dataset, 'contacts', 'role', role)


def update_extras_contacts_data(dataset, role, content):
    update_extras_json_list_data(dataset, 'contacts', 'role',
                                 role, content)


def get_extras_dates_data(dataset, role):
    return get_extras_json_list_data(dataset, 'dates', 'role', role)


def update_extras_dates_data(dataset, role, content):
    update_extras_json_list_data(dataset, 'dates', 'role',
                                 role, content)


def migrate_dates_field(dataset, from_field, to_field):
    '''extras.dates.<<from_field>> -> extras.<<to_field>>'''
    extras_dates = get_extras_dates_data(dataset, from_field)
    target_field = ds_utils.get_extras_field(dataset, to_field)

    if target_field is None and extras_dates:
        ds_utils.insert_new_extras_field(dataset, to_field,
                                         extras_dates.pop('date', ''), False)
        update_extras_dates_data(dataset, from_field,
                                 extras_dates)


def move_extras_contacts_address(dataset, role, new_role, contact_data=None):
    # load the data if no preloaded dict is available
    if contact_data is None:
        contact_data = get_extras_contacts_data(dataset, role)

    if contact_data is not None:
        if 'address' in contact_data:
            parsed_addr = addr_parse(contact_data['address'])
            keys = ['addressee', 'details', 'street', 'zip', 'city', 'country']

            # first, check if any of the new fields is present. If yes, skip
            # the movement to avoid corrupt datasets
            for k in keys:
                if ds_utils.get_extras_field(dataset, new_role + '_' + k):
                    return

            for k in keys:
                if k in parsed_addr:
                    ds_utils.insert_new_extras_field(dataset, new_role + '_' + k,
                                                     parsed_addr[k], False)

            addr_field_new = parsed_addr.get('unknown')
            if addr_field_new:
                contact_data['address'] = addr_field_new
                log_warn(dataset, u'The following address parts of role ' +
                         role + u' were not recognized: "' + addr_field_new
                         + u'"')
            else:
                del contact_data['address']

            update_extras_contacts_data(dataset, role, contact_data)


def unify_country_code(country):
    '''Takes a country value and tries to write a shorter country code.
    If nothing matches, the original value is left unchanged.'''
    # mapping based on existing data
    country_mapping = {
        'deu': 'de',
        'deutschland': 'de',
        'germany': 'de',
        'united kingdom': 'uk'
    }

    if country.lower() in country_mapping:
        return country_mapping[country.lower()].upper()
    elif country.lower() in country_mapping.keys():
        return country.upper()
    return country


def addr_parse(address):
    '''Takes an address string and returns a dict with appropriate fields.
    Everything which can't be mapped is stored in a field named "unknown".
    '''
    addr_data = dict()
    split_char = ','
    if address.count(';') > address.count(','):
        split_char = ';'

    # simple flag to handle the case where zip code and city are
    # separated
    expect_city = False

    count = 0
    address_parts = address.split(split_char)
    country = None

    if len(address_parts) > 0:
        # assume that the country is the last element and that it only
        # contains letters and spaces
        prt = address_parts[-1].strip()
        if re.match(r'^[A-Za-z ]+$', prt):
            # remember unified code for further processing, but store
            # the original value.
            country = unify_country_code(prt)
            addr_data['country'] = prt

    # zip regex contains everything needed to match a zip code.
    zip_regex = re.compile(r'^(\w{1,2}-)?\d{5}')
    street_num = r'[0-9/]+ ?([A-Za-z])?( ?- ?[0-9/]+ ?([A-Za-z])?)?'
    # match for numbers (may contain letters) and number ranges at the end,
    # e.g. "Beispielstrasse 20A-22".
    street_regex = re.compile(r'.* ' + street_num + r'$')

    if country == 'UK':
        zip_regex = re.compile(r'[A-Z0-9]{2,4} [A-Z0-9]{3}')
        street_regex = re.compile(r'^' + street_num + r' .*$')

    for part in address_parts:
        prt = part.strip()

        if count == len(address_parts) - 1 and country:
            # country code already processed. Ignore it such that no
            # other case matches.
            pass
        elif expect_city:
            addr_data['city'] = prt
            expect_city = False
        elif zip_regex.search(prt) and 'zip' not in addr_data:
            reg = zip_regex.search(prt)
            addr_data['zip'] = reg.group(0)
            city = prt.replace(reg.group(0), '').strip()
            if city:
                addr_data['city'] = city
            else:
                # field contained a single zip code, expect city as next field
                expect_city = True
        elif street_regex.match(prt) and 'street' not in addr_data:
            addr_data['street'] = prt
        elif count == 0 and prt:
            # if none of the above match and it's the first element, add it
            # as addressee field
            addr_data['addressee'] = prt
        elif count == 1 and prt:
            # if nothing else matches and it's the second element, add it
            # as details field
            addr_data['details'] = prt
        else:
            # store unparseable data
            if 'unknown' in addr_data:
                addr_data['unknown'] += ', ' + prt
            else:
                addr_data['unknown'] = prt

        count += 1

    return addr_data


def update_language_in(dataset, data_dict, key, desc_field):
    """Helper to update language country codes in the given data_dict,
    where the languate attribute is in data_dict[key]."""
    prefix = u'http://publications.europa.eu/resource/authority/language/'
    try:
        if not data_dict[key].startswith(prefix):
            language = pycountry.languages.get(alpha_2=data_dict[key])
            data_dict[key] = prefix + language.alpha_3.upper()
    except KeyError:
        log_error(dataset, 'INVALID: ' + desc_field + ': ' + data_dict[key])
