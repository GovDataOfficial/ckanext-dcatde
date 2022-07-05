from setuptools import setup, find_packages

VERSION = '4.7.0'

with open('base-requirements.txt') as f:
    required = [line.strip() for line in f]

setup(
    name='ckanext-dcatde',
    version=VERSION,
    description="Plugin to migrate to and provide DCAT-AP.de Profile",
    long_description='''\
    ''',
    classifiers=[],
    keywords='',
    author='Seitenbau GmbH',
    author_email='info@seitenbau.com',
    url='https://github.com/GovDataOfficial/ckanext-dcatde',
    license='AGPL',
    packages=find_packages(exclude=['ez_setup']),
    namespace_packages=['ckanext', 'ckanext.dcatde'],
    include_package_data=True,
    zip_safe=False,
    install_requires=required,
    entry_points='''
    [ckan.plugins]
    dcatde_rdf_harvester=ckanext.dcatde.harvesters.dcatde_rdf:DCATdeRDFHarvester

    dcatde=ckanext.dcatde.plugins:DCATdePlugin

    [ckan.rdf.profiles]
    dcatap_de=ckanext.dcatde.profiles:DCATdeProfile

    [paste.paster_command]
    dcatde_migrate = ckanext.dcatde.commands.migration:DCATdeMigrateCommand
    dcatde_themeadder = ckanext.dcatde.commands.themeadder:ThemeAdder
    triplestore = ckanext.dcatde.commands.triplestore:Triplestore
    ''',
    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
        ],
    },
)
