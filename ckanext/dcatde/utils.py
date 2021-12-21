from ckanext.dcatde.commands.click.triplestore import _reindex, _clean_triplestore_from_uris
from ckanext.dcatde.commands.click.migration import dcatde_migrate_command
from ckanext.dcatde.commands.click.themeadder import dcatde_themeadder_command

triplestore_reindex = _reindex
triplestore_clean = _clean_triplestore_from_uris
migration = dcatde_migrate_command
themeadder_command = dcatde_themeadder_command