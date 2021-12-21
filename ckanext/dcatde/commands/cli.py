from paste.script.command import Command
import logging
from ckan.cli import load_config as _get_config
from ckan.config.middleware import make_app
import routes
from six.moves.urllib.parse import urlparse

import ckan.logic as logic
import ckan.model as model

log = logging.getLogger(__name__)

class CkanCommand(Command):
    '''DEPRECATED - Instead use ckan.cli.cli.CkanCommand or extensions
    should use IClick.

    Base class for classes that implement CKAN paster commands to
    inherit.'''
    parser = Command.standard_parser(verbose=True)
    parser.add_option('-c', '--config', dest='config',
                      help='Config file to use.')
    parser.add_option('-f', '--file',
                      action='store',
                      dest='file_path',
                      help="File to dump results to (if needed)")
    default_verbosity = 1
    group_name = 'ckan'

    def _load_config(self, load_site_user=True):
        self.site_user = load_config(self.options.config, load_site_user)

def load_config(config, load_site_user=True):
    conf = _get_config(config)
    assert 'ckan' not in dir()  # otherwise loggers would be disabled
    # We have now loaded the config. Now we can import ckan for the
    # first time.
    from ckan.config.environment import load_environment
    load_environment(conf)

    # Set this internal test request context with the configured environment so
    # it can be used when calling url_for from the CLI.
    global _cli_test_request_context

    app = make_app(conf)
    try:
        flask_app = app.apps['flask_app']._wsgi_app
    except AttributeError as e:
        print(e)
        flask_app = app._wsgi_app
    _cli_test_request_context = flask_app.test_request_context()

    site_user = None
    if model.user_table.exists() and load_site_user:
    
        site_user = logic.get_action('get_site_user')({'ignore_auth': True}, {})

    ## give routes enough information to run url_for
    parsed = urlparse(conf.get('ckan.site_url', 'http://0.0.0.0'))
    request_config = routes.request_config()
    request_config.host = parsed.netloc + parsed.path
    request_config.protocol = parsed.scheme

    return site_user
