""" register plugin things here """
from ckan import plugins as p
from ckanext.dcatde.commands import click


class DCATdePlugin(p.SingletonPlugin):
    """ for now, this class does nothing """
    p.implements(p.IClick)

    def get_commands(self):
        return click.get_commands()
