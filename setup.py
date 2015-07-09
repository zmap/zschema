from setuptools import setup

setup(
    name = "zson",
    description = "A JSON serializer for Kombu (and therefore also Celery) that "
                  "supports encoding objects by defining to_json and from_json methods.",
    version = "0.0.1",
    license = "Apache License, Version 2.0",
    author = "Zakir Durumeric",
    author_email = "zakird@gmail.com",
    maintainer = "Zakir Durumeric",
    maintainer_email = "zakird@gmail.com",

    keywords = "python json schema bigquery elastic search",

    install_requires = [
        "python-dateutil"
    ]

    packages = [
        "zschema"
    ],

    entry_points={
        'console_scripts': [
            'zschema = zschema.cli:main',
        ]
    }
)

