import codecs
import os
import re

from setuptools import setup, find_packages


###############################################################################

NAME = 'pg2kinesis'
PACKAGES = find_packages(where='.')

if not PACKAGES:
    raise RuntimeError('Package NOT FOUND!')

META_PATH = os.path.join('pg2kinesis', '__init__.py')
KEYWORDS = ['logical replication', 'kinesis', 'database']
CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Natural Language :: English',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: Implementation :: CPython',
    'Topic :: Utilities',
]
INSTALL_REQUIRES = [
    'aws_kinesis_agg>=1.0.1',
    'boto3>=1.4.1',
    'botocore>=1.4.64',
    'click>=6.3.0',
    'protobuf>=3.0.0',
    'psycopg2>=2.7.1',
]

###############################################################################

HERE = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(HERE, *parts), 'rb', 'utf-8') as f:
        return f.read()


META_FILE = read(META_PATH)


def find_meta(meta):
    """
    Extract __*meta*__ from META_FILE.
    """
    meta_match = re.search(
        r'^__{meta}__ = [\'"]([^\'"]*)[\'"]'.format(meta=meta),
        META_FILE, re.M
    )
    if meta_match:
        return meta_match.group(1)
    raise RuntimeError('Unable to find __{meta}__ string.'.format(meta=meta))


VERSION = find_meta('version')
URI = find_meta('uri')
LONG = read('README.rst')

if __name__ == "__main__":
    setup(
        name=NAME,
        description=find_meta("description"),
        license=find_meta("license"),
        url=URI,
        version=VERSION,
        author=find_meta("author"),
        author_email=find_meta("email"),
        maintainer=find_meta("author"),
        maintainer_email=find_meta("email"),
        keywords=KEYWORDS,
        long_description=LONG,
        packages=PACKAGES,
        zip_safe=True,
        classifiers=CLASSIFIERS,
        install_requires=INSTALL_REQUIRES,
        entry_points={
            'console_scripts': ['pg2kinesis=pg2kinesis.__main__:main'],
        }
    )
