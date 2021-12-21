from ckanext.dcatde.commands.click import triplestore
from ckanext.dcatde.commands.click import migration
from ckanext.dcatde.commands.click import themeadder

def get_commands():
    return [
        triplestore.triplestore,
        migration.dcatde_migrate,
        themeadder.dcatde_themeadder
    ]
