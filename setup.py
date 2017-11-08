# -*- coding: utf-8 -*-

from setuptools import setup

import os.path

base_dir = os.path.dirname(__file__)

about = dict()
with open(os.path.join(base_dir, "zschema", "__init__.py")) as f:
    exec(f.read(), about)

setup(
    name = "zschema",
    description = "A schema language for JSON documents that allows validation and compilation into various database engines",
    version = about["__version__"],
    license = about["__license__"],
    author = about["__author__"],
    author_email = about["__email__"],
    keywords = "python json schema bigquery elasticsearch",

    install_requires = [
        "python-dateutil",
    ],

    packages = [
        "zschema",
    ],

    entry_points={
        'console_scripts': [
            'zschema = zschema.__main__:main',
        ]
    },

    tests_require = [ 'nose' ],
    test_suite = 'nose.collector'
)
