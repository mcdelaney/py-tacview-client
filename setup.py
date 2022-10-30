# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['tacview_client']

package_data = \
{'': ['*']}

install_requires = \
['Cython>=0.29.21,<0.30.0',
 'SQLAlchemy==1.4.0b1',
 'asyncpg>=0.20.1,<0.21.0',
 'click>=7.1.2,<8.0.0',
 'pytz>=2020.5,<2021.0',
 'typer>=0.3.2,<0.4.0']

entry_points = \
{'console_scripts': ['tacview = tacview_client.cli:app']}

setup_kwargs = {
    'name': 'tacview-client',
    'version': '0.1.75',
    'description': '',
    'long_description': None,
    'author': 'mcdelaney',
    'author_email': 'mcdelaney@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': 'https://github.com/mcdelaney/py-tacview-client.git',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'entry_points': entry_points,
    'python_requires': '>=3.7,<3.11',
}
from build import *
build(setup_kwargs)

setup(**setup_kwargs)
