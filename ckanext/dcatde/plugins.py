""" register plugin things here """
import ckan.plugins.toolkit as tk
from ckan import plugins as p

class DCATdePlugin(p.SingletonPlugin):
    """ Init Plugin """

    if tk.check_ckan_version(min_version='2.9.0'):
        p.implements(p.IClick)
        # IClick
        def get_commands(self):
            from ckanext.dcatde.commands.cli import get_commands  # pylint: disable=import-outside-toplevel
            return get_commands()
