"""
Commoun utils for dataset dicts
"""
import json

from ckan import model
from ckan.plugins import toolkit as tk
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import or_, and_, not_


EXTRA_KEY_HARVESTED_PORTAL = 'metadata_harvested_portal'


def get_extras_field(dataset, name):
    """ Getter for extra fields """
    for field in dataset['extras']:
        if field['key'] == name:
            return field

    return None


def process_value(value, as_list=False):
    """ Process value if as_list is True """
    if as_list:
        return json.dumps([value])

    return value


def insert_new_extras_field(dataset, key, value, as_list=False):
    """ Add new field for extra fields """
    dataset['extras'].insert(0, {u'value': process_value(value, as_list), u'key': key})


def set_extras_field(dataset, key, value, as_list=False):
    """ Setter for extra fields """
    current = get_extras_field(dataset, key)

    if current:
        current['value'] = process_value(value, as_list)
    else:
        insert_new_extras_field(dataset, key, value, as_list)


def delete_extras_field(dataset, name):
    """ Delete extra field by name """
    dataset['extras'].remove(get_extras_field(dataset, name))


def rename_extras_field(dataset, name_old, name_new, as_list):
    """ Rename an extra field """
    field = get_extras_field(dataset, name_old)

    if field is not None:
        insert_new_extras_field(dataset, name_new, field['value'], as_list)
        delete_extras_field(dataset, name_old)
        return True

    return False


def insert_resource_extra(resource_dict, key, value):
    """
    Insert not from the resource (default) schema known fields into undocumented __extras dict.
    """

    if '__extras' not in resource_dict:
        resource_dict['__extras'] = {}

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


def gather_dataset_ids(include_private=True):
    """Collects all dataset ids to reindex."""
    package_obj_found = {}
    # pylint: disable=no-member

    # read all package IDs to reindex
    query = model.Session.query(model.Package.id, model.Package.owner_org).distinct() \
        .outerjoin(model.PackageExtra, model.PackageExtra.package_id == model.Package.id) \
        .filter(model.Package.type != 'harvest') \
        .filter(model.Package.state == model.State.ACTIVE)

    if "harvest" in tk.aslist(tk.config.get("ckan.plugins")):
        import ckanext.harvest.model as harvest_model  # pylint: disable=import-outside-toplevel

        package_extra_alias = aliased(model.PackageExtra)

        # read orgs related to a harvest source
        subquery_harvest_orgs = model.Session.query(model.Group.id).distinct() \
            .join(model.Package, model.Package.owner_org == model.Group.id) \
            .join(harvest_model.HarvestSource, harvest_model.HarvestSource.id == model.Package.id) \
            .filter(model.Package.state == model.State.ACTIVE) \
            .filter(harvest_model.HarvestSource.active.is_(True)) \
            .filter(model.Group.state == model.State.ACTIVE) \
            .filter(model.Group.is_organization.is_(True))

        query = query.filter(or_(model.Package.owner_org.notin_(subquery_harvest_orgs),
                    and_(model.Package.owner_org.in_(subquery_harvest_orgs),
                         not_(model.Session.query(model.Package.id)
                              .filter(and_(model.Package.id == package_extra_alias.package_id,
                                           package_extra_alias.state == model.State.ACTIVE,
                                           package_extra_alias.key == EXTRA_KEY_HARVESTED_PORTAL))
                              .exists()))))

    if not include_private:
        query = query.filter(model.Package.private.is_(False))
    # pylint: enable=no-member
    for row in query:
        package_obj_found[row[0]] = row[1]

    return package_obj_found
