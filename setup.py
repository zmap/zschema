from setuptools import setup

setup(
    name = "zschema",
    description = "A schema language for JSON documents that allows validation and compilation into various database engines",
    version = "0.0.32",
    license = "Apache License, Version 2.0",
    author = "Zakir Durumeric",
    author_email = "zakird@gmail.com",
    maintainer = "Zakir Durumeric",
    maintainer_email = "zakird@gmail.com",

    keywords = "python json schema bigquery elastic search",

    install_requires = [
        "python-dateutil"
    ],

    packages = [
        "zschema"
    ],

    entry_points={
        'console_scripts': [
            'zschema = zschema.cli:main',
        ]
    }
)

