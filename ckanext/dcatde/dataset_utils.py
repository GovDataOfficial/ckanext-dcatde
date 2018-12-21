"""
Commoun utils for dataset dicts
"""
import json


def get_extras_field(dataset, name):
    for field in dataset['extras']:
        if field['key'] == name:
            return field

    return None


def process_value(value, as_list=False):
    if as_list:
        return json.dumps([value])

    return value


def insert_new_extras_field(dataset, key, value, as_list=False):
    dataset['extras'].insert(0, {u'value': process_value(value, as_list), u'key': key})


def set_extras_field(dataset, key, value, as_list=False):
    current = get_extras_field(dataset, key)

    if current:
        current['value'] = process_value(value, as_list)
    else:
        insert_new_extras_field(dataset, key, value, as_list)


def delete_extras_field(dataset, name):
    dataset['extras'].remove(get_extras_field(dataset, name))


def rename_extras_field(dataset, name_old, name_new, as_list):
    field = get_extras_field(dataset, name_old)

    if field is not None:
        insert_new_extras_field(dataset, name_new, field['value'], as_list)
        delete_extras_field(dataset, name_old)
        return True

    return False


def insert_resource_extra(resource_dict, key, value):
    if '__extras' not in resource_dict:
        resource_dict['__extras'] = dict()

    resource_dict['__extras'][key] = value


def insert(dataset_dict, key, value, isextra):
    """
    Adds the key, value pair as extras or on top-level if it
    is non-null.
    If key already exists, the current value is overwritten.
    """
    if value:
        if isextra:
            set_extras_field(dataset_dict, key, value)
        else:
            dataset_dict[key] = value
