from setuptools import setup
import zschema

setup(
    name = "zschema",
    description = "A schema language for JSON documents that allows validation and compilation into various database engines",
    version = zschema.__version__,
    license = zschema.__license__,
    author = zschema.__author__,
    author_email = zschema.__email__,
    keywords = "python json schema bigquery elastic search",

    install_requires = [
        "python-dateutil"
    ],

    packages = [
        "zschema"
    ],

    entry_points={
        'console_scripts': [
            'zschema = zschema.__main__:main',
        ]
    }
)

