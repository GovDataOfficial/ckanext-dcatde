""" register plugin things here """
import ckan.plugins.toolkit as tk
from ckan import plugins as p
import ckanext.dcatde.commands.cli as cli


class DCATdePlugin(p.SingletonPlugin):
    """ Init Plugin """

    p.implements(p.IClick)
    # IClick
    def get_commands(self):
        return cli.get_commands()
