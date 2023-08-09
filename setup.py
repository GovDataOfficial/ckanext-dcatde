from setuptools import setup, find_packages

VERSION = '6.1.0'

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
    author='SEITENBAU GmbH',
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
    ''',
    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
        ],
    },
)
